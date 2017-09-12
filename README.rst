**ATTENTION: This module is not yet fully tested, use it at your own risk!**

This script migrates the database of an Indico 1.2.x server to **version 2.0**.


Migrating to Indico 2.0
-----------------------

Please refer to the `Upgrade Guide <https://docs.getindico.io/en/latest/installation/upgrade_legacy/>`_ for
instructions on how to upgrade from Indico 1.2 to 2.0.


Migration settings
------------------

The migration command accepts a series of options that you will have to adjust according to your current Indico setup
and whichever way you'd like to organize your new installation. We will go over them one by one, please **read carefully**!

The basic migration command consists of::

    $ indico-migrate <sqlalchemy-uri> <zodb-uri> [--rb-zodb-uri <rb-zodb-uri>]


==================
``sqlalchemy-uri``
==================
    This is the URI of your new SQLAlchemy database. If it's located in your local machine, it will look like this::

        postgresql:///indico

    Otherwise, it will be something like ``postgresql://<username>:<password>@<hostname>:<port>/indico``.


============
``zodb-uri``
============
    This is the URI of your Indico 1.2 ZODB. It should normally look like this::

        zeo://localhost:9675/indico

    If you're running it on a remote server, it will become ``zeo://<username>:<password>@<hostname>:<port>/indico``

    You can also choose to access the database file directly, instead of going through the ZEO service::

        file:///opt/indico/db/Data.fs

    This is slightly slower than the previous option but can be very useful if all you have is a database file and you
    don't want to set up a ZEO server.


===========================
``--rb-zodburi`` (optional)
===========================
    This option only applies if you were running Indico's Room Booking system and wish to migrate room and reservation
    information. The URI follows the same rules of ``zodb-uri``. In most cases it will be something like::

        zeo://localhost:9676/indico


This is followed by the relevant options from the following list:

``--system-user-id`` (optional)
===============================
    The "system user" is a user that performs automatic operations and which will also be used whenever, during the
    migration process, Indico doesn't manage to figure out who was responsible for a given action.

    *For instance, in Indico < 2.0 we were not keeping track of whoever submitted a paper revision. In 2.0 that is
    enforced, which is why during the migration we set all revisions as if they were authored by the system user.*

    If you already had an Indico user that you were using for "bot" actions, you can specify its (numeric) ID using this
    option. If that's not the case, you can safely ignore it and a new user will be created automatically.

    Example::

        $ indico-migrate postgresql:///indico2 zeo://127.0.0.1:9675 -v


``--default-email`` (mandatory)
===============================
    This is an e-mail address that will be used whenever Indico finds invalid addresses it cannot correct.
    Unfortunately, really old versions of Indico didn't validate e-mail addresses that well, which is why we need a
    placeholder e-mail to use in case we find an address we cannot understand.
    E.g. ``broken-indico-identity@myorganization.org``.


``--ldap-provider-name`` (optional)
====================================
    This is the name of the LDAP provider that will be used in your new config. Existing LDAP identities will be mapped
    to it. The best choice at this point is to set it to something like ``<your-organization>-ldap``
    (e.g. ``cern-ldap``). Write down this choice, **you will need it** when configuring Indico 2.0.


``--default-group-provider`` (mandatory)
========================================
    This is the name of the LDAP provider that will be used to resolve any groups in your new config. Existing LDAP
    groups will be mapped to the provider with this ID. It will be almost always the same value as that of
    ``--ldap-provider-name``. Once again, write it down, **you will need it** when configuring Indico 2.0.


``--archive-dir`` (mandatory)
=============================
    This is the ``ArchiveDir`` that was set in your old ``indico.conf`` (in version 1.2).


``--storage-backend`` (mandatory)
=================================
    This is the name of the storage backend where migrated files will be kept. This option was added since in Indico
    2.0 you will be able to set more than one storage backend (thus being able to split your files across different
    folders and even storage technology). The setup process needs to know the ID you will give to the storage backend
    where files imported from 1.2 will be kept. Since the directory structure changes slightly in 2.0, we recommend
    that you set this to something like ``fs-legacy``. Whatever you choose, write it down since **you'll need it** when
    configuring 2.0.


``--symlink-target`` and ``--symlink-backend`` (optional)
=========================================================
    These two options are closely related and require each other.

    The sanitization of file names was quite poor in some early versions of Indico. This could lead to files with
    non-UTF8 names, which would be difficult for Indico 2.0 to deal with. Instead of renaming those files (which is not
    as simple as it may seem), we chose to instead symlink them and store the name of the link instead. If these options
    are specified, symlinks to weirdly-named files will be kept in the directory indicated by ``--symlink-target``. They
    will be associated with the storage backend ID specified in ``--symlink-backend``.

    **NOTE:** This means that in the ``STORAGE_BACKENDS`` option of your new ``indico.conf`` you will have something
    like::

        STORAGE_BACKENDS = {
            ...
            'fs-legacy-symlinks`: '/opt/indico/archive/legacy-symlinks'
        }


``--photo-path`` (optional)
===========================
    If ``--rb-zodb-uri`` was specified, this is an optional directory (path) where Indico will be able to find photos
    of each room. Indico will look inside two directories: ``small_photos`` (thumbnails) and ``large_photos`` and import
    existing files (``<room_canonical_name>.jpg``) into the database.


``--reference-type`` (optional, multiple)
=========================================
    If you were storing "Report Numbers" (now known as "External References"), specify here the IDs of the systems that
    were being used. Those should correspond to the keys in your ``ReportNumberSystems`` dictionary (Indico 1.2 config).
    The result should be somethink like ``--reference-type CDS --reference-type EDMS ...``


``--default-currency`` (mandatory)
==================================
    This is the code of the currency unit that will be used by default in your server. E.g. ``USD`` or ``EUR``.


``--ignore-local-accounts`` (optional flag)
===========================================
    This option is meant for servers that were at some point using local accounts (username + password) but have since
    adopted another authentication method (LDAP + SSO, for instance). If you don't need local accounts anymore and don't
    want to move the old usernames and password to the new DB (which is probably a good idea), then set this option.
    This will also save you some migration time.


``--migrate-broken-events`` (optional flag)
===========================================
    This option will import events that were previously broken due to not being associated with a valid category.
    Such events usually only exist if the old database had events imported from CDSAgenda.
    If this flag is enabled and any such events exist, a new top-level category named 'Lost & Found' will be created
    and the events stored in there.


==============
Other settings
==============

These less used settings are meant mainly for debugging purposes. You shouldn't normally use them unless you know what
you're doing.

``--no-gui`` (optional flag)
============================
    This option will disable the curses-like "graphical" interface, using plain text instead.


``--verbose`` (optional flag)
=============================
    This flag increases the verbosity of the Indico migration command. The amount of information can be overwhelming.


``--dblog`` (optional flag)
===========================
    If this option is specified, the migration command will contact the
    `Indico DB logger <https://github.com/indico/indico/blob/master/bin/utils/db_log.py>`_ running in the local machine
    and log every request that is made to the PostgreSQL server.


``--debug`` (optional flag)
===========================
    This option will launch the migration in debug mode, which means that the user will be given a debugger shell
    when something goes wrong.


``--avoid-storage-check`` (optional flag)
=========================================
    **DANGER!**
    By specifying this option, you're telling Indico it doesn't need to check if a file really exists when migrating it.
    This will result in a faster migration but as well in **possible data inconsistency and incomplete information**.



``--save-restore`` (optional flag)
==================================
    This option triggers a dump of all intermediate migration data that is kept in memory to a file on disk, called
    ``indico-migration.yaml``, whenever the migration fails. This allows the process to be resumed from the point
    at which it failed.


``--restore-file`` (optional flag)
==================================
    **DANGER!**
    This option takes a file path as argument. The file in question should be a dump proced with ``--save-restore`` and
    which will be loaded to memory. The global migration steps that had been performed at the time of the failure will
    be skipped.
