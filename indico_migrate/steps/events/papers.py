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
from collections import defaultdict
from datetime import timedelta

from pytz import utc

from indico.core.db import db
from indico.modules.events.features.util import set_feature_enabled
from indico.modules.events.models.events import EventType
from indico.modules.events.papers.models.competences import PaperCompetence
from indico.modules.events.papers.models.files import PaperFile
from indico.modules.events.papers.models.papers import Paper
from indico.modules.events.papers.models.review_questions import PaperReviewQuestion
from indico.modules.events.papers.models.review_ratings import PaperReviewRating
from indico.modules.events.papers.models.reviews import PaperAction, PaperReview, PaperReviewType
from indico.modules.events.papers.models.revisions import PaperRevision, PaperRevisionState
from indico.modules.events.papers.models.templates import PaperTemplate
from indico.modules.events.papers.settings import PaperReviewingRole, paper_reviewing_settings
from indico.util.fs import secure_filename

from indico_migrate.steps.events import EventMigrationStep
from indico_migrate.util import LocalFileImporterMixin, convert_to_unicode, strict_now_utc


CPR_NO_REVIEWING = 1
CPR_CONTENT_REVIEWING = 2
CPR_LAYOUT_REVIEWING = 3
CPR_CONTENT_AND_LAYOUT_REVIEWING = 4

JUDGMENT_STATE_PAPER_ACTION_MAP = {
    1: PaperAction.accept,
    2: PaperAction.to_be_corrected,
    3: PaperAction.reject
}

JUDGMENT_STATE_REVISION_MAP = {
    1: PaperRevisionState.accepted,
    2: PaperRevisionState.to_be_corrected,
    3: PaperRevisionState.rejected
}

STATE_COLOR_MAP = {
    PaperRevisionState.submitted: 'blue',
    PaperRevisionState.accepted: 'green',
    PaperRevisionState.to_be_corrected: 'yellow',
    PaperRevisionState.rejected: 'red'
}


def _invert_mapping(mapping):
    result = defaultdict(list)
    for user, contribs in mapping.iteritems():
        for contrib in contribs:
            result[contrib].append(user)
    return result


def _to_utc(dt):
    return dt.astimezone(utc) if dt else None


def _translate_notif_options(pr, options):
    return {PaperReviewingRole[role] for role, (attr, default) in options.viewitems() if getattr(pr, attr, default)}


def _review_color(review, text):
    return '%[{}]{}%[reset]'.format({
        PaperAction.accept: 'green',
        PaperAction.to_be_corrected: 'yellow',
        PaperAction.reject: 'red'
    }[review.proposed_action], text)


class EventPaperReviewingImporter(LocalFileImporterMixin, EventMigrationStep):
    step_id = 'paper'

    def __init__(self, *args, **kwargs):
        super(EventPaperReviewingImporter, self).__init__(*args, **kwargs)
        self._set_config_options(**kwargs)

    def migrate(self):
        self.legacy_contrib_revision_map = {}
        self.legacy_contrib_last_revision_map = dict()
        self.checksum_map = {}
        self.legacy_question_map = {}

        self.pr = getattr(self.conf, '_confPaperReview', None)
        if self.event.type_ != EventType.conference or not self.pr:
            return

        with db.session.no_autoflush:
            self._migrate_feature()
            self._migrate_settings()
            self._migrate_event_roles()
            self._migrate_questions()
            self._migrate_templates()
            self._migrate_competences()
            self._migrate_papers()
            db.session.flush()

    def _migrate_feature(self):
        if self.pr._choice != CPR_NO_REVIEWING:
            set_feature_enabled(self.event, 'papers', True)

    def _migrate_settings(self):
        pr = self.pr

        role_add = _translate_notif_options(pr, {
            'layout_reviewer': ('_enableEditorEmailNotif', False),
            'content_reviewer': ('_enableReviewerEmailNotif', False),
            'judge': ('_enableRefereeEmailNotif', False)
        })

        contrib_assignment = _translate_notif_options(pr, {
            'layout_reviewer': ('_enableEditorEmailNotifForContribution', False),
            'content_reviewer': ('_enableReviewerEmailNotifForContribution', False),
            'judge': ('_enableRefereeEmailNotifForContribution', False)
        })

        paper_submission = _translate_notif_options(pr, {
            'layout_reviewer': ('_enableAuthorSubmittedMatEditorEmailNotif', True),
            'content_reviewer': ('_enableAuthorSubmittedMatReviewerEmailNotif', True),
            'judge': ('_enableAuthorSubmittedMatRefereeEmailNotif', False)
        })

        paper_reviewing_settings.set_multi(self.event, {
            'start_dt': _to_utc(pr._startSubmissionDate),
            'end_dt': _to_utc(pr._endSubmissionDate),
            'judge_deadline': _to_utc(pr._defaultRefereeDueDate),
            'content_reviewer_deadline': _to_utc(pr._defaultReviwerDueDate),
            'layout_reviewer_deadline': _to_utc(pr._defaultEditorDueDate),
            'enforce_judge_deadline': False,
            'enforce_layout_reviewer_deadline': False,
            'enforce_content_reviewer_deadline': False,
            'content_reviewing_enabled': pr._choice in {CPR_CONTENT_REVIEWING, CPR_CONTENT_AND_LAYOUT_REVIEWING},
            'layout_reviewing_enabled': pr._choice in {CPR_LAYOUT_REVIEWING, CPR_CONTENT_AND_LAYOUT_REVIEWING},
            'scale_lower': -3,
            'scale_upper': 3,

            # notifications
            'notify_on_added_to_event': role_add,
            'notify_on_assigned_contrib': contrib_assignment,
            'notify_on_paper_submission': paper_submission,
            'notify_judge_on_review': (getattr(pr, '_enableEditorSubmittedRefereeEmailNotif', True) or
                                       getattr(pr, '_enableReviewerSubmittedRefereeEmailNotif', True)),
            'notify_author_on_judgment': (pr._enableRefereeJudgementEmailNotif or pr._enableEditorJudgementEmailNotif or
                                          pr._enableReviewerJudgementEmailNotif)
        })

    def _migrate_event_roles(self):
        for avatar in self.pr._paperReviewManagersList:
            self.event.update_principal(self.global_ns.avatar_merged_user[avatar.id], add_roles={'paper_manager'},
                                        quiet=True)
        for avatar in self.pr._refereesList:
            self.event.update_principal(self.global_ns.avatar_merged_user[avatar.id], add_roles={'paper_judge'},
                                        quiet=True)
        for avatar in self.pr._reviewersList:
            self.event.update_principal(self.global_ns.avatar_merged_user[avatar.id],
                                        add_roles={'paper_content_reviewer'}, quiet=True)
        for avatar in self.pr._editorsList:
            self.event.update_principal(self.global_ns.avatar_merged_user[avatar.id],
                                        add_roles={'paper_layout_reviewer'}, quiet=True)

    def _migrate_questions(self):
        for n, q in enumerate(self.pr._reviewingQuestions, 1):
            question = PaperReviewQuestion(text=q._text, type=PaperReviewType.content, position=n, event=self.event)
            self.event.paper_review_questions.append(question)
            self.legacy_question_map[q] = question

        for n, q in enumerate(self.pr._layoutQuestions, 1):
            question = PaperReviewQuestion(text=q._text, type=PaperReviewType.layout, position=n, event=self.event)
            self.event.paper_review_questions.append(question)
            self.legacy_question_map[q] = question

    def _migrate_templates(self):
        for __, old_tpl in self.pr._templates.viewitems():
            old_filename = convert_to_unicode(old_tpl._Template__file.name)
            storage_backend, storage_path, size, md5 = self._get_local_file_info(old_tpl._Template__file)
            if storage_path is None:
                self.print_error('%[red!]File not found on disk; skipping it [{}]'.format(old_filename))
                continue

            filename = secure_filename(old_filename, 'attachment')
            content_type = mimetypes.guess_type(old_filename)[0] or 'application/octet-stream'
            tpl = PaperTemplate(filename=filename, content_type=content_type, size=size, md5=md5,
                                storage_backend=storage_backend, storage_file_id=storage_path,
                                name=convert_to_unicode(old_tpl._Template__name) or old_filename,
                                description=convert_to_unicode(old_tpl._Template__description))
            self.event.paper_templates.append(tpl)

    def _migrate_competences(self):
        competence_map = {}
        for avatar, competences in self.pr._userCompetences.viewitems():
            user = self.global_ns.avatar_merged_user[avatar.id]
            if user.id in competence_map:
                # add to existing list, which SQLAlchemy will commit
                competence_map[user.id] += competences
            elif competences:
                competence_map[user.id] = competences
                competence = PaperCompetence(user=user, competences=competences)
                self.event.paper_competences.append(competence)

    def _migrate_paper_roles(self, old_contrib, contribution):
        self._migrate_role(old_contrib, PaperReviewingRole.content_reviewer, self.contrib_reviewers,
                           contribution.paper_content_reviewers)
        self._migrate_role(old_contrib, PaperReviewingRole.judge, self.contrib_referees, contribution.paper_judges)
        self._migrate_role(old_contrib, PaperReviewingRole.layout_reviewer, self.contrib_editors,
                           contribution.paper_layout_reviewers)

    def _migrate_role(self, contrib, role, mapping, target_list):
        for avatars in mapping[contrib]:
            if not isinstance(avatars, list):
                avatars = [avatars]
            for avatar in avatars:
                user = self.global_ns.avatar_merged_user[avatar.id]
                target_list.add(user)
                self.print_info('{} %[white!]-> %[blue]{}%[reset]: %[green]{}'
                                .format(contrib.id, user, role.name))

    def _migrate_review(self, contribution, old_judgment, review_type):
        # Consider legacy custom states the same as "to be corrected"
        proposed_action = JUDGMENT_STATE_PAPER_ACTION_MAP.get(int(old_judgment._judgement._id),
                                                              PaperAction.to_be_corrected)
        review = PaperReview(user=self.global_ns.avatar_merged_user[old_judgment._author.id],
                             comment=convert_to_unicode(old_judgment._comments),
                             type=review_type, proposed_action=proposed_action,
                             created_dt=_to_utc(old_judgment._submissionDate))
        for old_answer in old_judgment._answers:
            old_question = old_answer._question
            try:
                question = self.legacy_question_map[old_question]
            except KeyError:
                self.print_warning('%[yellow!]Answer to deleted question {} has been ignored [{}, {}]'
                                   .format(old_question._id, contribution, review_type))
                continue

            assert old_answer._rbValue >= 0 and old_answer._rbValue <= 6
            rating = PaperReviewRating(question=question, value=(old_answer._rbValue - 3))
            review.ratings.append(rating)
        return review

    def _migrate_revisions(self, old_contrib, contribution, rm):
        revision_dts = set()
        self.print_info('%[white!]{}%[reset]'.format(contribution))

        self.file_checksums = defaultdict()

        # Here we iterate over what the legacy PR mode calls "Reviews" (our `PaperRevisions`)
        for n, old_revision in enumerate(rm._versioning, 1):
            old_judgment = old_revision._refereeJudgement
            old_content_reviews = old_revision._reviewerJudgements.values()
            old_layout_review = old_revision._editorJudgement

            # keep track of the last legacy revision, so that we can come back to it
            # during paper file migration
            self.legacy_contrib_last_revision_map[old_contrib] = old_revision

            # skip revisions that haven't been submitted by the author
            if not old_revision._isAuthorSubmitted:
                # ... except if said revision has been judged before being marked as submitted (!)
                if old_judgment._submitted:
                    self.print_warning('%[yellow!]Revision judged without being submitted! [{}: {}]'
                                       .format(contribution, old_revision._version))
                else:
                    continue

            # It seems contradictory, but 'submitted' in legacy means that there is a final decision
            # We'll ignore legacy custom states and use TBC
            state = (JUDGMENT_STATE_REVISION_MAP.get(int(old_judgment._judgement._id),
                                                     PaperRevisionState.to_be_corrected)
                     if old_judgment._submitted
                     else PaperRevisionState.submitted)
            judge = self.global_ns.avatar_merged_user[old_judgment._author.id] if old_judgment._submitted else None
            judgment_dt = _to_utc(old_judgment._submissionDate) if old_judgment._submitted else None
            # Legacy didn't keep track of the submission date (nor submitter for that matter)
            # we're taking the most recent uploaded file and using that one
            # if there are no files, the event's end date will be used
            revision = PaperRevision(state=state, submitter=self.system_user, judge=judge, judgment_dt=judgment_dt,
                                     judgment_comment=convert_to_unicode(old_judgment._comments))
            self.legacy_contrib_revision_map[(old_contrib, old_revision._version)] = revision

            # Then we'll add the layout and content reviews
            review_colors = ''

            for old_content_review in old_content_reviews:
                if old_content_review._submitted:
                    review = self._migrate_review(contribution, old_content_review, PaperReviewType.content)
                    revision.reviews.append(review)
                    review_colors += _review_color(review, 'C')
            if old_layout_review._submitted:
                review = self._migrate_review(contribution, old_layout_review, PaperReviewType.layout)
                revision.reviews.append(review)
                review_colors += _review_color(review, 'L')
            contribution._paper_revisions.append(revision)

            self.print_info('\tRevision %[blue!]{}%[reset] %[white,{}]  %[reset] {}'.format(
                n, STATE_COLOR_MAP[state], review_colors))

            last_file = self._migrate_paper_files(old_contrib, contribution, old_revision, revision)
            submitted_dt = _to_utc(last_file.created_dt) if last_file else min(self.event.end_dt, strict_now_utc())

            # some dates may be duplicates (shouldn't happen if CRC is used, though)
            while submitted_dt in revision_dts:
                submitted_dt += timedelta(seconds=1)
            revision_dts.add(submitted_dt)
            revision.submitted_dt = submitted_dt

            db.session.flush()

    def _migrate_papers(self):
        self.contrib_reviewers = _invert_mapping(self.pr._reviewerContribution)
        self.contrib_referees = _invert_mapping(self.pr._refereeContribution)
        self.contrib_editors = _invert_mapping(self.pr._editorContribution)

        for contrib_id, old_contrib in self.conf.contributions.iteritems():
            if old_contrib not in self.event_ns.legacy_contribution_map:
                self.print_warning('%[yellow!]Contribution {} not found in event'.format(contrib_id))
                continue

            contribution = self.event_ns.legacy_contribution_map[old_contrib]
            revisions = Paper(contribution).revisions
            self._migrate_paper_roles(old_contrib, contribution)

            review_manager = getattr(old_contrib, '_reviewManager', None)
            if review_manager:
                self._migrate_revisions(old_contrib, contribution, review_manager)

            reviewing = getattr(old_contrib, 'reviewing', None)

            # if there were no materials attached to the contribution or no revisions, we're done
            if not reviewing or not revisions:
                continue

            # these are the resources that correspond to the latest revision
            for resource in reviewing._Material__resources.itervalues():
                self._migrate_resource(contribution, revisions[-1], resource,
                                       getattr(reviewing, '_modificationDS', strict_now_utc()), set())

    def _migrate_paper_files(self, old_contrib, contribution, old_revision, revision):
        reviewing = getattr(old_contrib, 'reviewing', None)
        last_file = None
        ignored_checksums = set()

        if not getattr(old_revision, '_materials', None):
            return
        assert len(old_revision._materials) == 1
        for resource in old_revision._materials[0]._Material__resources.itervalues():
            res_file = self._migrate_resource(contribution, revision, resource,
                                              getattr(reviewing, '_modificationDS', strict_now_utc()),
                                              ignored_checksums)
            if res_file:
                last_file = res_file

        # if a revision has no files (because they've all been ignored), then keep around a copy of each
        if not revision.files and ignored_checksums:
            for checksum in ignored_checksums:
                paper_file = self.checksum_map[checksum]
                paper_file._contribution = contribution
                revision.files.append(paper_file)
                self.print_warning('%[yellow!]File {} (rev. {}) reinstated'.format(paper_file.filename, revision.id))

        return last_file

    def _migrate_resource(self, contribution, revision, resource, created_dt, ignored_checksums):
        storage_backend, storage_path, size, md5 = self._get_local_file_info(resource, force_access=True)
        content_type = mimetypes.guess_type(resource.fileName)[0] or 'application/octet-stream'

        if not storage_path:
            self.print_error("%[red!]File not accessible [{}]".format(convert_to_unicode(resource.fileName)))
            return

        paper_file = PaperFile(filename=resource.fileName, content_type=content_type,
                               size=size, md5=md5, storage_backend=storage_backend,
                               storage_file_id=storage_path, created_dt=created_dt)

        # check whether the same file has been uploaded to a subsequent revision
        if md5:
            self.checksum_map[md5] = paper_file
            collision = self.file_checksums.get(md5)
            if collision:
                ignored_checksums.add(md5)
                self.print_warning('%[yellow!]File {} (rev. {}) already in revision {}'.format(
                    convert_to_unicode(resource.fileName), revision.id if revision else None, collision.id))
                return
            else:
                self.file_checksums[md5] = revision
        else:
            self.print_error("%[red!]File not accessible; can't MD5 it [{}]"
                             .format(convert_to_unicode(paper_file.filename)))

        paper_file._contribution = contribution
        paper_file.paper_revision = revision
        db.session.add(paper_file)
        return paper_file
