# This file is part of Indico.
# Copyright (C) 2002 - 2017 European Organization for Nuclear Research (CERN).
#
# Indico is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.
#
# Indico is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Indico; if not, see <http://www.gnu.org/licenses/>.

from __future__ import division, unicode_literals

import mimetypes
import textwrap
from collections import defaultdict
from datetime import timedelta
from uuid import uuid4

from indico.core.db import db
from indico.core.db.sqlalchemy.descriptions import RenderMode
from indico.modules.events.abstracts.models.abstracts import Abstract, AbstractState
from indico.modules.events.abstracts.models.comments import AbstractComment
from indico.modules.events.abstracts.models.email_logs import AbstractEmailLogEntry
from indico.modules.events.abstracts.models.email_templates import AbstractEmailTemplate
from indico.modules.events.abstracts.models.fields import AbstractFieldValue
from indico.modules.events.abstracts.models.files import AbstractFile
from indico.modules.events.abstracts.models.persons import AbstractPersonLink
from indico.modules.events.abstracts.models.review_questions import AbstractReviewQuestion
from indico.modules.events.abstracts.models.review_ratings import AbstractReviewRating
from indico.modules.events.abstracts.models.reviews import AbstractAction, AbstractReview
from indico.modules.events.abstracts.settings import (BOACorrespondingAuthorType, BOASortField,
                                                      abstracts_reviewing_settings, abstracts_settings, boa_settings)
from indico.modules.events.contributions.models.fields import ContributionField
from indico.modules.events.contributions.models.persons import AuthorType
from indico.modules.events.contributions.models.types import ContributionType
from indico.modules.events.features.util import set_feature_enabled
from indico.modules.users.models.users import UserTitle
from indico.util.date_time import as_utc
from indico.util.fs import secure_filename

from indico_migrate.steps.events import EventMigrationStep
from indico_migrate.util import LocalFileImporterMixin, convert_to_unicode, strict_sanitize_email


class EventAbstractImporter(LocalFileImporterMixin, EventMigrationStep):
    step_id = 'abstract'

    CONDITION_MAP = {'NotifTplCondAccepted': AbstractState.accepted,
                     'NotifTplCondRejected': AbstractState.rejected,
                     'NotifTplCondMerged': AbstractState.merged}

    STATE_MAP = {'AbstractStatusSubmitted': AbstractState.submitted,
                 'AbstractStatusWithdrawn': AbstractState.withdrawn,
                 'AbstractStatusAccepted': AbstractState.accepted,
                 'AbstractStatusRejected': AbstractState.rejected,
                 'AbstractStatusMerged': AbstractState.merged,
                 'AbstractStatusDuplicated': AbstractState.duplicate,
                 # obsolete states
                 'AbstractStatusUnderReview': AbstractState.submitted,
                 'AbstractStatusProposedToReject': AbstractState.submitted,
                 'AbstractStatusProposedToAccept': AbstractState.submitted,
                 'AbstractStatusInConflict': AbstractState.submitted}

    JUDGED_STATES = {AbstractState.accepted, AbstractState.rejected, AbstractState.duplicate, AbstractState.merged}

    ACTION_MAP = {'AbstractAcceptance': AbstractAction.accept,
                  'AbstractRejection': AbstractAction.reject,
                  'AbstractReallocation': AbstractAction.change_tracks,
                  'AbstractMarkedAsDuplicated': AbstractAction.mark_as_duplicate}

    SUBMISSION_NOTIFICATION_BODY = textwrap.dedent('''
        We've received your abstract "{abstract_title}" to which we have assigned id #{abstract_id}.

        Kind regards,
        The organizers of {event_title}
    ''').strip()

    def __init__(self, *args, **kwargs):
        super(EventAbstractImporter, self).__init__(*args, **kwargs)
        self.default_email = kwargs.get('default_email')
        self._set_config_options(**kwargs)

    def teardown(self):
        self.fix_sequences('event_abstracts', {'abstracts'})

    @property
    def amgr(self):
        return self.conf.abstractMgr

    def migrate(self):
        self.question_map = {}
        self.email_template_map = {}
        self.legacy_warnings_shown = set()
        self.old_scale = None
        self.new_scale = None

        duration = self.amgr._submissionEndDate - self.amgr._submissionStartDate
        if (not self.amgr._activated and not self.amgr._abstracts and not self.amgr._notifTpls and
                duration < timedelta(minutes=1) and not self.conf.program):
            return
        with db.session.no_autoflush:
            self._migrate_feature()
            self._migrate_boa_settings()
            self._migrate_settings()
            self._migrate_review_settings()
            self._migrate_email_templates()
            self._migrate_contribution_types()
            self._migrate_contribution_fields()
            self._migrate_abstracts()
        db.session.flush()

    def _migrate_feature(self):
        if self.amgr._activated:
            set_feature_enabled(self.event, 'abstracts', True)

    def _migrate_contribution_types(self):
        name_map = {}
        for old_ct in self.conf._contribTypes.itervalues():
            name = convert_to_unicode(old_ct._name)
            existing = name_map.get(name.lower())
            if existing is not None:
                self.print_warning('%[yellow]Duplicate contribution type name: {}'.format(name))
                self.event_ns.legacy_contribution_type_map[old_ct] = existing
                continue
            ct = ContributionType(name=name, description=convert_to_unicode(old_ct._description))
            name_map[name.lower()] = ct
            if not self.quiet:
                self.print_info('%[cyan]Contribution type%[reset] {}'.format(ct.name))
            self.event_ns.legacy_contribution_type_map[old_ct] = ct
            self.event.contribution_types.append(ct)

    def _migrate_boa_settings(self):
        boa_config = self.conf._boa
        sort_field_map = {'number': 'id', 'none': 'id', 'name': 'abstract_title', 'sessionTitle': 'session_title',
                          'speakers': 'speaker', 'submitter': 'id'}
        try:
            sort_by = sort_field_map.get(boa_config._sortBy, boa_config._sortBy)
        except AttributeError:
            sort_by = 'id'
        corresponding_author = getattr(boa_config, '_correspondingAuthor', 'submitter')
        boa_settings.set_multi(self.event, {
            'extra_text': convert_to_unicode(boa_config._text),
            'sort_by': BOASortField[sort_by],
            'corresponding_author': BOACorrespondingAuthorType[corresponding_author],
            'show_abstract_ids': bool(getattr(boa_config, '_showIds', False))
        })

    def _migrate_settings(self):
        start_dt = self._naive_to_aware(self.amgr._submissionStartDate)
        end_dt = self._naive_to_aware(self.amgr._submissionEndDate)
        modification_end_dt = (self._naive_to_aware(self.amgr._modifDeadline)
                               if getattr(self.amgr, '_modifDeadline', None)
                               else None)
        assert start_dt < end_dt
        if modification_end_dt and modification_end_dt - end_dt < timedelta(minutes=1):
            if modification_end_dt != end_dt:
                self.print_warning('Ignoring mod deadline ({} > {})'.format(end_dt, modification_end_dt))
            modification_end_dt = None
        abstracts_settings.set_multi(self.event, {
            'start_dt': start_dt,
            'end_dt': end_dt,
            'modification_end_dt': modification_end_dt,
            'announcement': convert_to_unicode(self.amgr._announcement),
            'announcement_render_mode': RenderMode.html,
            'allow_multiple_tracks': bool(getattr(self.amgr, '_multipleTracks', True)),
            'tracks_required': bool(getattr(self.amgr, '_tracksMandatory', False)),
            'allow_attachments': bool(getattr(self.amgr, '_attachFiles', False)),
            'allow_speakers': bool(getattr(self.amgr, '_showSelectAsSpeaker', True)),
            'speakers_required': bool(getattr(self.amgr, '_selectSpeakerMandatory', True)),
            'authorized_submitters': set(filter(None, map(self.user_from_legacy,
                                                          getattr(self.amgr, '_authorizedSubmitter', []))))
        })

    def _migrate_review_settings(self):
        try:
            old_settings = self.conf._confAbstractReview
        except AttributeError:
            return
        self.old_scale = (int(old_settings._scaleLower), int(old_settings._scaleHigher))
        if self.old_scale[1] - self.old_scale[0] <= 20:
            self.new_scale = self.old_scale
        else:
            self.new_scale = (0, 10)
        abstracts_reviewing_settings.set_multi(self.event, {
            'scale_lower': self.new_scale[0],
            'scale_upper': self.new_scale[1],
            'allow_convener_judgment': bool(getattr(old_settings, '_canReviewerAccept', False))
        })
        for pos, old_question in enumerate(old_settings._reviewingQuestions, 1):
            self._migrate_question(old_question, pos=pos)

    def _migrate_question(self, old_question, pos=None, is_deleted=False):
        assert old_question not in self.question_map
        question = AbstractReviewQuestion(position=pos, text=convert_to_unicode(old_question._text),
                                          is_deleted=is_deleted)
        self.question_map[old_question] = question
        self.event.abstract_review_questions.append(question)
        return question

    def _convert_email_template(self, tpl):
        placeholders = {'abstract_URL': 'abstract_url',
                        'abstract_id': 'abstract_id',
                        'abstract_review_comments': 'judgment_comment',
                        'abstract_session': 'abstract_session',
                        'abstract_title': 'abstract_title',
                        'abstract_track': 'abstract_track',
                        'conference_URL': 'event_url',
                        'conference_title': 'event_title',
                        'contribution_URL': 'contribution_url',
                        'contribution_type': 'contribution_type',
                        'merge_target_abstract_id': 'target_abstract_id',
                        'merge_target_abstract_title': 'target_abstract_title',
                        'merge_target_submitter_family_name': 'target_submitter_last_name',
                        'merge_target_submitter_first_name': 'target_submitter_first_name',
                        'primary_authors': 'primary_authors',
                        'submitter_family_name': 'submitter_last_name',
                        'submitter_first_name': 'submitter_first_name',
                        'submitter_title': 'submitter_title'}
        tpl = convert_to_unicode(tpl)
        for old, new in placeholders.iteritems():
            tpl = tpl.replace('%({})s'.format(old), '{%s}' % new)
        return tpl.replace('%%', '%')

    def _migrate_email_templates(self):
        assert bool(dict(self.amgr._notifTpls.iteritems())) == bool(self.amgr._notifTplsOrder)
        pos = 1
        for old_tpl in self.amgr._notifTplsOrder:
            title = convert_to_unicode(old_tpl._name)
            body = self._convert_email_template(old_tpl._tplBody)
            subject = self._convert_email_template(old_tpl._tplSubject) or 'Your Abstract Submission'
            reply_to_address = strict_sanitize_email(old_tpl._fromAddr, self.default_email)
            extra_cc_emails = sorted(set(filter(None, map(strict_sanitize_email, old_tpl._ccAddrList))))
            include_submitter = any(x.__class__.__name__ == 'NotifTplToAddrSubmitter' for x in old_tpl._toAddrs)
            include_authors = any(x.__class__.__name__ == 'NotifTplToAddrPrimaryAuthors' for x in old_tpl._toAddrs)
            if not body:
                self.print_warning('%[yellow!]Template "{}" has no body'.format(title))
                continue
            tpl = AbstractEmailTemplate(title=title,
                                        position=pos,
                                        reply_to_address=reply_to_address,
                                        subject=subject,
                                        body=body,
                                        extra_cc_emails=extra_cc_emails,
                                        include_submitter=include_submitter,
                                        include_authors=include_authors,
                                        include_coauthors=bool(getattr(old_tpl, '_CAasCCAddr', False)))
            pos += 1
            self.print_info('%[white!]Email Template:%[reset] {}'.format(tpl.title))
            self.event.abstract_email_templates.append(tpl)
            self.email_template_map[old_tpl] = tpl
            rules = []
            for old_cond in old_tpl._conditions:
                # state
                try:
                    state = self.CONDITION_MAP[old_cond.__class__.__name__]
                except KeyError:
                    self.print_error('%[red!]Invalid condition type: {}'.format(old_cond.__class__.__name__))
                    continue
                if state == AbstractState.rejected:
                    track = contrib_type = any
                else:
                    # track
                    if getattr(old_cond, '_track', '--any--') == '--any--':
                        track = any
                    elif getattr(old_cond, '_track', '--any--') == '--none--':
                        track = None
                    else:
                        try:
                            track = self.event_ns.track_map.get(old_cond._track)
                        except KeyError:
                            self.print_warning('%[yellow!]Invalid track: {}'.format(old_cond._track))
                            continue
                    # contrib type
                    if hasattr(old_cond, '_contrib_type_id'):
                        contrib_type_id = old_cond._contrib_type_id
                        if contrib_type_id == '--any--':
                            contrib_type = any
                        elif contrib_type_id == '--none--':
                            contrib_type = None
                        else:
                            contrib_type = self.event.contribution_types.filter_by(id=contrib_type_id).one()
                    elif not hasattr(old_cond, '_contribType'):
                        contrib_type = any
                        self.print_warning('%[yellow]No contrib type data, using any [{}]'
                                           .format(old_cond.__dict__))
                    else:
                        contrib_type = None
                        self.print_error('%[red!]Legacy contribution type not found: {}'
                                         .format(old_cond._contribType))
                _any_str = '%[green]any%[reset]'
                self.print_success('%[white!]Condition:%[reset] {} | {} | {}'
                                   .format(state.name, track if track is not any else _any_str,
                                           contrib_type if contrib_type is not any else _any_str))
                rule = {'state': [state.value]}
                if track is not any:
                    rule['track'] = [track.id if track else None]
                if contrib_type is not any:
                    rule['contribution_type'] = [contrib_type.id if contrib_type else None]
                rules.append(rule)
            if not rules:
                self.print_warning('%[yellow]Template "{}" has no rules'.format(tpl.title), always=False)
            tpl.rules = rules

        # submission notification
        reply_to_address = strict_sanitize_email(self.conf._supportInfo._email, self.default_email)
        try:
            old_sn = self.amgr._submissionNotification
        except AttributeError:
            emails = []
        else:
            emails = old_sn._toList + old_sn._ccList
        tpl = AbstractEmailTemplate(title='Abstract submitted', position=pos,
                                    reply_to_address=reply_to_address,
                                    subject='Abstract Submission confirmation (#{abstract_id})',
                                    body=self.SUBMISSION_NOTIFICATION_BODY,
                                    extra_cc_emails=sorted(set(filter(None, map(strict_sanitize_email, emails)))),
                                    include_submitter=True,
                                    rules=[{'state': [AbstractState.submitted.value]}])
        self.event.abstract_email_templates.append(tpl)

    def _migrate_abstract(self, old_abstract):
        submitter = self.user_from_legacy(old_abstract._submitter._user, system_user=True)
        submitted_dt = old_abstract._submissionDate
        modified_dt = (old_abstract._modificationDate
                       if (submitted_dt - old_abstract._modificationDate) > timedelta(seconds=10)
                       else None)
        description = getattr(old_abstract, '_fields', {}).get('content', '')
        description = convert_to_unicode(getattr(description, 'value', description))  # str or AbstractFieldContent

        type_ = old_abstract._contribTypes[0]
        type_id = None
        try:
            type_id = self.event_ns.legacy_contribution_type_map[type_].id if type_ else None
        except KeyError:
            self.print_warning('Abstract {} - invalid contrib type {}, setting to None'
                               .format(old_abstract._id, convert_to_unicode(getattr(type_, '_name', str(type_)))))

        abstract = Abstract(friendly_id=int(old_abstract._id),
                            title=convert_to_unicode(old_abstract._title),
                            description=description,
                            submitter=submitter,
                            submitted_dt=submitted_dt,
                            submitted_contrib_type_id=type_id,
                            submission_comment=convert_to_unicode(old_abstract._comments),
                            modified_dt=modified_dt)
        self.print_info('%[white!]Abstract %[cyan]{}%[reset]: {}'.format(abstract.friendly_id, abstract.title))
        self.event.abstracts.append(abstract)
        self.event_ns.abstract_map[old_abstract] = abstract

        accepted_type_id = None
        accepted_track_id = None

        old_contribution = getattr(old_abstract, '_contribution', None)
        if old_contribution:
            assert old_contribution.__class__.__name__ == 'AcceptedContribution'
            if old_abstract._currentStatus.__class__.__name__ == 'AbstractStatusAccepted':
                old_contrib_type = old_abstract._currentStatus._contribType
                try:
                    accepted_type_id = (self.event_ns.legacy_contribution_type_map[old_contrib_type].id
                                        if old_contrib_type else None)
                except KeyError:
                    self.print_warning(
                        '%[yellow!]Contribution {} - invalid contrib type {}, setting to None'
                        .format(old_contribution.id, convert_to_unicode(old_contrib_type._name)))

                old_accepted_track = old_abstract._currentStatus._track
                accepted_track_id = int(old_accepted_track.id) if old_accepted_track else None

        if old_contribution and old_contribution.id is not None:
            self.event_ns.legacy_contribution_abstracts[old_contribution] = abstract

        try:
            accepted_track = (self.event_ns.track_map_by_id.get(accepted_track_id)
                              if accepted_track_id is not None
                              else None)
        except KeyError:
            self.print_error('%[yellow!]Abstract #{} accepted in invalid track #{}'
                             .format(abstract.friendly_id, accepted_track_id))
            accepted_track = None

        # state
        old_state = old_abstract._currentStatus
        old_state_name = old_state.__class__.__name__
        self.event_ns.old_abstract_state_map[abstract] = old_state
        abstract.state = self.STATE_MAP[old_state_name]

        if abstract.state == AbstractState.accepted:
            abstract.accepted_contrib_type_id = accepted_type_id
            abstract.accepted_track = accepted_track

        if abstract.state in self.JUDGED_STATES:
            abstract.judge = self.user_from_legacy(old_state._responsible, system_user=True)
            abstract.judgment_dt = as_utc(old_state._date)

        # files
        for old_attachment in getattr(old_abstract, '_attachments', {}).itervalues():
            storage_backend, storage_path, size, md5 = self._get_local_file_info(old_attachment)
            if storage_path is None:
                self.print_error('%[red!]File not found on disk; skipping it [{}]'
                                 .format(convert_to_unicode(old_attachment.fileName)))
                continue
            content_type = mimetypes.guess_type(old_attachment.fileName)[0] or 'application/octet-stream'
            filename = secure_filename(convert_to_unicode(old_attachment.fileName), 'attachment')
            attachment = AbstractFile(filename=filename, content_type=content_type, size=size, md5=md5,
                                      storage_backend=storage_backend, storage_file_id=storage_path)
            abstract.files.append(attachment)

        # internal comments
        for old_comment in old_abstract._intComments:
            comment = AbstractComment(user=self.user_from_legacy(old_comment._responsible, system_user=True),
                                      text=convert_to_unicode(old_comment._content),
                                      created_dt=old_comment._creationDate,
                                      modified_dt=old_comment._modificationDate)
            abstract.comments.append(comment)

        # tracks
        reallocated = set(r._track for r in getattr(old_abstract, '_trackReallocations', {}).itervalues())
        for old_track in old_abstract._tracks.values():
            abstract.reviewed_for_tracks.add(self.event_ns.track_map.get(old_track))
            if old_track not in reallocated:
                abstract.submitted_for_tracks.add(self.event_ns.track_map.get(old_track))

        # reviews/judgments
        self._migrate_abstract_reviews(abstract, old_abstract)
        # persons
        self._migrate_abstract_persons(abstract, old_abstract)
        # email log
        self._migrate_abstract_email_log(abstract, old_abstract)

        # contribution/abstract fields
        abstract.field_values = list(self._migrate_abstract_field_values(old_abstract))
        return abstract

    def _migrate_abstracts(self):
        for zodb_abstract in self.amgr._abstracts.itervalues():
            self._migrate_abstract(zodb_abstract)

        if self.event.abstracts:
            self.event._last_friendly_contribution_id = max(a.friendly_id for a in self.event.abstracts)

        # merges/duplicates
        for abstract in self.event.abstracts:
            old_state = self.event_ns.old_abstract_state_map.get(abstract)
            if abstract.state == AbstractState.merged:
                abstract.merged_into = self.event_ns.abstract_map.get(old_state._target)
            elif abstract.state == AbstractState.duplicate:
                abstract.duplicate_of = self.event_ns.abstract_map.get(old_state._original)

        # mark-as-duplicate judgments
        for review, old_abstract in self.event_ns.as_duplicate_reviews.viewitems():
            try:
                review.proposed_related_abstract = self.event_ns.abstract_map[old_abstract]
            except KeyError:
                self.print_error('%[yellow!]Abstract #{} marked as duplicate of invalid abstract #{}'
                                 .format(review.abstract.friendly_id, old_abstract._id))
                # delete the review; it would violate our CHECKs
                review.abstract = None
                # not needed but avoids some warnings about the object not in the session
                review.track = None
                review.user = None

    def _migrate_abstract_reviews(self, abstract, old_abstract):
        if not hasattr(old_abstract, '_trackJudgementsHistorical'):
            self.print_warning('%[blue!]Abstract {} %[yellow]had no judgment history!%[reset]'
                               .format(old_abstract._id))
            return

        history = old_abstract._trackJudgementsHistorical
        if not hasattr(history, 'iteritems'):
            self.print_warning('Abstract {} had corrupt judgment history ({}).'.format(old_abstract._id, history))
            return
        for track_id, old_judgments in history.iteritems():
            seen_judges = set()
            for old_judgment in old_judgments:
                if old_judgment is None:
                    continue
                if old_judgment.__class__.__name__ == 'AbstractUnMarkedAsDuplicated':
                    # we don't have "unmarked as duplicate" anymore
                    continue

                try:
                    track = self.event_ns.track_map_by_id[int(old_judgment._track.id)]
                except KeyError:
                    self.print_warning('%[blue!]Abstract {} %[yellow]judged in invalid track {}%[reset]'.format(
                        old_abstract._id, int(old_judgment._track.id)))
                    continue

                judge = (self.global_ns.avatar_merged_user.get(old_judgment._responsible.id)
                         if old_judgment._responsible else None)
                if not judge:
                    self.print_warning('%[blue!]Abstract {} %[yellow]had an empty judge ({})!%[reset]'
                                       .format(old_abstract._id, old_judgment))
                    continue
                elif judge in seen_judges:
                    self.print_warning("%[blue!]Abstract {}: %[yellow]judge '{}' seen more than once ({})!%[reset]"
                                       .format(old_abstract._id, judge, old_judgment))
                    continue

                seen_judges.add(judge)

                try:
                    created_dt = as_utc(old_judgment._date)
                except AttributeError:
                    created_dt = self.event.start_dt
                review = AbstractReview(created_dt=created_dt,
                                        proposed_action=self.ACTION_MAP[old_judgment.__class__.__name__],
                                        comment=convert_to_unicode(old_judgment._comment))
                if review.proposed_action == AbstractAction.accept:
                    review.proposed_contribution_type = self.event_ns.legacy_contribution_type_map.get(
                        old_judgment._contribType)
                elif review.proposed_action == AbstractAction.change_tracks:
                    review.proposed_tracks = {self.event_ns.track_map[t] for t in old_judgment._proposedTracks
                                              if t in self.event_ns.track_map}
                elif review.proposed_action == AbstractAction.mark_as_duplicate:
                    self.event_ns.as_duplicate_reviews[review] = old_judgment._originalAbst

                review.user = judge
                review.track = track

                answered_questions = set()
                for old_answer in getattr(old_judgment, '_answers', []):
                    if old_answer._question in answered_questions:
                        self.print_warning("%[blue!]Abstract {}: %[yellow]question answered more than once!".format(
                            abstract.friendly_id))
                        continue
                    try:
                        question = self.question_map[old_answer._question]
                    except KeyError:
                        question = self._migrate_question(old_answer._question, is_deleted=True)
                        self.print_warning("%[blue!]Abstract {}: %[yellow]answer for deleted question".format(
                            abstract.friendly_id))
                    rating = AbstractReviewRating(question=question, value=self._convert_scale(old_answer))
                    review.ratings.append(rating)
                    answered_questions.add(old_answer._question)

                abstract.reviews.append(review)

    def _migrate_abstract_field_values(self, old_abstract):
        fields = dict(getattr(old_abstract, '_fields', {}))
        fields.pop('content', None)
        for field_id, field_content in fields.iteritems():
            value = convert_to_unicode(getattr(field_content, 'value', field_content))
            if not value:
                continue
            try:
                new_field = self.event_ns.legacy_contribution_field_map[field_id]
            except KeyError:
                self.print_warning('%[yellow!]Contribution field "{}" does not exist'.format(field_id))
                continue
            new_value = self._process_abstract_field_value(field_id, value, new_field)
            if new_value:
                if not self.quiet:
                    self.print_info('%[green] - [field]%[reset] {}: {}'.format(new_field.title, new_value.data))
                yield new_value

    def _process_abstract_field_value(self, old_field_id, old_value, new_field):
        if new_field.field_type == 'text':
            data = convert_to_unicode(old_value)
            return AbstractFieldValue(contribution_field=new_field, data=data)
        elif new_field.field_type == 'single_choice':
            data = self.event_ns.legacy_field_option_id_map.get(old_field_id, int(old_value))
            return AbstractFieldValue(contribution_field=new_field, data=data)
        else:
            raise ValueError('Unexpected field type: {}'.format(new_field.field_type))

    def _convert_scale(self, old_answer):
        old_value = float(old_answer._value)
        old_min, old_max = self.old_scale
        new_min, new_max = self.new_scale
        new_value = int(round((((old_value - old_min) * (new_max - new_min)) / (old_max - old_min)) + new_min))
        if int(old_value) != new_value:
            self.print_warning('Adjusted value: %[cyan]{} [{}..{}] %[white]==> %[cyan!]{} [{}..{}]'
                               .format(old_value, self.old_scale[0], self.old_scale[1],
                                       new_value, self.new_scale[0], self.new_scale[1]), always=False)
        return new_value

    def _migrate_abstract_email_log(self, abstract, zodb_abstract):
        for old_entry in zodb_abstract._notifLog._entries:
            email_template = self.email_template_map.get(old_entry._tpl)
            email_template_name = email_template.title if email_template else convert_to_unicode(old_entry._tpl._name)
            entry = AbstractEmailLogEntry(email_template=email_template, sent_dt=old_entry._date,
                                          user=self.user_from_legacy(old_entry._responsible),
                                          recipients=[], subject='<not available>', body='<not available>',
                                          data={'_legacy': True, 'template_name': email_template_name or '<unnamed>'})
            abstract.email_logs.append(entry)

    def _migrate_abstract_persons(self, abstract, zodb_abstract):
        old_persons = defaultdict(lambda: {'is_speaker': False, 'author_type': AuthorType.none})
        for old_person in zodb_abstract._coAuthors:
            old_persons[old_person]['author_type'] = AuthorType.secondary
        for old_person in zodb_abstract._primaryAuthors:
            old_persons[old_person]['author_type'] = AuthorType.primary
        for old_person in zodb_abstract._speakers:
            old_persons[old_person]['is_speaker'] = True

        person_links_by_person = {}
        for person, roles in old_persons.iteritems():
            person_link = self._person_link_from_legacy(person)
            person_link.author_type = roles['author_type']
            person_link.is_speaker = roles['is_speaker']
            try:
                existing = person_links_by_person[person_link.person]
            except KeyError:
                person_links_by_person[person_link.person] = person_link
            else:
                author_type = AuthorType.get_highest(existing.author_type, person_link.author_type)
                new_flags = '{}{}{}'.format('P' if person_link.author_type == AuthorType.primary else '_',
                                            'S' if person_link.author_type == AuthorType.secondary else '_',
                                            's' if person_link.is_speaker else '_')
                existing_flags = '{}{}{}'.format('P' if existing.author_type == AuthorType.primary else '_',
                                                 'S' if existing.author_type == AuthorType.secondary else '_',
                                                 's' if existing.is_speaker else '_')
                if person_link.author_type == author_type and existing.author_type != author_type:
                    # the new one has the higher author type -> use that one
                    person_link.author_type = author_type
                    person_link.is_speaker |= existing.is_speaker
                    person_links_by_person[person_link.person] = person_link
                    self.print_warning('%[blue!]Abstract {}: %[yellow]Author {} already exists '
                                       '(%[magenta]{} [{}] %[yellow]/ %[green]{} [{}]%[yellow])'
                                       .format(abstract.friendly_id, existing.person.full_name, existing.full_name,
                                               existing_flags, person_link.full_name, new_flags))
                    existing.person = None  # cut the link to an already-persistent object
                else:
                    # the existing one has the higher author type -> use that one
                    existing.author_type = author_type
                    existing.is_speaker |= person_link.is_speaker
                    self.print_warning('%[blue!]Abstract {}: %[yellow]Author {} already exists '
                                       '(%[green]{} [{}]%[yellow] / %[magenta]{} [{}]%[yellow])'
                                       .format(abstract.friendly_id, existing.person.full_name, existing.full_name,
                                               existing_flags, person_link.full_name, new_flags))
                    person_link.person = None  # cut the link to an already-persistent object

        abstract.person_links.extend(person_links_by_person.viewvalues())

    def _migrate_contribution_fields(self):
        pos = 0

        try:
            afm = self.amgr._abstractFieldsMgr
        except AttributeError:
            return

        content_field = None
        for field in afm._fields:
            # it may happen that there is a second 'content' field (old version schemas)
            # in that case, let's use the first one as description and keep the second one as a field
            if field._id == 'content' and not content_field:
                content_field = field
            else:
                pos += 1
                self._migrate_contribution_field(field, pos)

        if not content_field:
            self.print_warning('%[yellow!]Event has no content field!%[reset]')
            return

        def _positive_or_none(value):
            try:
                value = int(value)
            except (TypeError, ValueError):
                return None
            return value if value > 0 else None

        limitation = getattr(content_field, '_limitation', 'chars')
        settings = {
            'is_active': bool(content_field._active),
            'is_required': bool(content_field._isMandatory),
            'max_words': _positive_or_none(content_field._maxLength) if limitation == 'words' else None,
            'max_length': _positive_or_none(content_field._maxLength) if limitation == 'chars' else None
        }
        if settings != abstracts_settings.defaults['description_settings']:
            abstracts_settings.set(self.event, 'description_settings', settings)

    def _migrate_contribution_field(self, old_field, position):
        field_type = old_field.__class__.__name__
        if field_type in ('AbstractTextAreaField', 'AbstractInputField', 'AbstractField'):
            multiline = field_type == 'AbstractTextAreaField' or (field_type == 'AbstractField' and
                                                                  getattr(old_field, '_type', 'textarea') == 'textarea')
            limitation = getattr(old_field, '_limitation', 'chars')
            field_data = {
                'max_length': int(old_field._maxLength) if limitation == 'chars' else None,
                'max_words': int(old_field._maxLength) if limitation == 'words' else None,
                'multiline': multiline
            }
            field_type = 'text'
        elif field_type == 'AbstractSelectionField':
            options = []
            for opt in old_field._options:
                uuid = unicode(uuid4())
                self.event_ns.legacy_field_option_id_map[old_field._id, int(opt.id)] = uuid
                options.append({'option': convert_to_unicode(opt.value), 'id': uuid, 'is_deleted': False})
            for opt in old_field._deleted_options:
                uuid = unicode(uuid4())
                self.event_ns.legacy_field_option_id_map[old_field._id, int(opt.id)] = uuid
                options.append({'option': convert_to_unicode(opt.value), 'id': uuid, 'is_deleted': True})
            field_data = {'options': options, 'display_type': 'select'}
            field_type = 'single_choice'
        else:
            self.print_error('Unrecognized field type {}'.format(field_type))
            return
        if old_field._id in self.event_ns.legacy_contribution_field_map:
            self.print_warning("%[yellow!]There is already a field with legacy_id '{}')!%[reset]".format(old_field._id))
            return
        field = ContributionField(event=self.event, field_type=field_type, is_active=old_field._active,
                                  title=convert_to_unicode(old_field._caption), is_required=old_field._isMandatory,
                                  field_data=field_data, position=position, legacy_id=old_field._id)
        self.event_ns.legacy_contribution_field_map[old_field._id] = field
        if not self.quiet:
            self.print_info('%[green]Contribution field%[reset] {}'.format(field.title))

    def _person_link_from_legacy(self, old_person):
        person = self.event_person_from_legacy(old_person)
        person_link = AbstractPersonLink(person=person)
        data = dict(first_name=convert_to_unicode(old_person._firstName),
                    last_name=convert_to_unicode(old_person._surName),
                    _title=self.USER_TITLE_MAP.get(getattr(old_person, '_title', ''), UserTitle.none),
                    affiliation=convert_to_unicode(old_person._affilliation),
                    address=convert_to_unicode(old_person._address),
                    phone=convert_to_unicode(old_person._telephone))
        person_link.populate_from_dict(data)
        return person_link
