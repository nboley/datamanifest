import os
from tqdm import tqdm
import sys

import datamanifest._logging as logging
from datamanifest.datamanifest import (
    DataManifest,
    DataManifestWriter,
    KeyAlreadyExistsError,
    FileAlreadyExistsError,
    random_string,
)

logger = None

def _find_all_files_and_directories(files_and_directories_to_add, dm_fname):
    rv = []
    # strip off the common prefix
    files_and_directories_to_add = [os.path.normpath(x) for x in files_and_directories_to_add]
    common_prefix = os.path.commonprefix(files_and_directories_to_add) if len(files_and_directories_to_add) > 1 else ''
    for path in files_and_directories_to_add:
        logger.info(f"Adding '{path}' to {dm_fname}")
        if os.path.isfile(path):
            rv.append((path[len(common_prefix):], path))
        elif os.path.isdir(path):
            for root, _, fnames in os.walk(path):
                for fname in fnames:
                    # we add 1 to the common prefix length to strip off the '/'
                    key = os.path.normpath(os.path.join(root[len(common_prefix):], fname))
                    path = os.path.normpath(os.path.abspath(os.path.join(root, fname)))
                    rv.append((key, path))
        else:
            raise ValueError(f"Passed file or directory '{file_or_dir}' does not appear to be a file or directory.")
    return rv


def _add_subdirectory(dm, files_and_directories_to_add, resume=False, dry_run=False, include_base_dir_in_key=False):
    """Add all of the data in 'directory_to_add' to 'manifest_fname'.

    Keys are set to the data suffix. e.g. if we have a file /scratch/data/sub/data.tsv
    and directory_to_add='/scratch/data', then the key will be 'data/sub/data.tsv'.
    """
    for key, full_path in tqdm(
            _find_all_files_and_directories(files_and_directories_to_add, dm.fname),
            file=sys.stdout,
            desc=f"Add files",
    ):
        try:
            logger.info(f"Adding '{key}' to {dm.fname} for {full_path}")
            if not dry_run:
                dm.add(key, full_path)
        except KeyAlreadyExistsError:
            if not resume:
                raise
            else:
                logger.warning(f"'{key}' already exists in manifest, but resume is set so skipping.")


def add_subdirectory_main(manifest_fname, directory_to_add, checkout_prefix, dry_run, resume):
    dm = DataManifestWriter(manifest_fname, checkout_prefix=checkout_prefix)
    _add_subdirectory(
        dm, directory_to_add, dry_run=dry_run, include_base_dir_in_key=False, resume=resume,
    )


def create_new_manifest_main(manifest_fname, directory_to_add, checkout_prefix, remote_datastore_uri, dry_run, resume):
    assert manifest_fname.endswith(".data_manifest.tsv"), "Should look like NAME.data_manifest.tsv"
    if os.path.exists(manifest_fname):
        raise FileAlreadyExistsError(f"'{manifest_fname}' already exists.")

    try:
        # we create a temp manifest name in case we run into an error during creation. If we succesfully
        # finish the operation without raising an exception then we rename the manifest file. Otherwise,
        # we delete the temporary version.
        head, tail = os.path.split(manifest_fname)
        tmp_manifest_fname = os.path.join(head, f"tmp.{random_string(16)}.{tail}")
        dm = DataManifestWriter.new(tmp_manifest_fname, remote_datastore_uri=remote_datastore_uri, checkout_prefix=checkout_prefix)
        _add_subdirectory(
            dm, directory_to_add, dry_run=dry_run, include_base_dir_in_key=False, resume=resume,
        )
    except:
        if os.path.exists(tmp_manifest_fname):
            os.remove(tmp_manifest_fname)
        if os.path.exists(DataManifestWriter.local_config_path(tmp_manifest_fname)):
            os.remove(DataManifestWriter.local_config_path(tmp_manifest_fname))
        raise
    else:
        if dry_run:
            os.remove(tmp_manifest_fname)
            os.remove(DataManifestWriter.local_config_path(".local_config"))
        else:
            os.rename(tmp_manifest_fname, manifest_fname)
            os.rename(DataManifestWriter.local_config_path(tmp_manifest_fname), DataManifestWriter.local_config_path(manifest_fname))


def add_main(manifest_fname, key, path, notes):
    dm = DataManifestWriter(manifest_fname)
    dm.add(key, path, notes=notes)


def update_main(manifest_fname, key, path, notes):
    dm = DataManifestWriter(manifest_fname)
    dm.update(key, path, notes=notes)


def delete_main(manifest_fname, key, delete_from_datastore, force_delete=False):
    dm = DataManifestWriter(manifest_fname)
    if delete_from_datastore and not force_delete:
        i_am_sure = input(
            f"WARNING: you have chosen to delete '{key}' from the datastore.\n"
            "This action CANNOT BE UNDONE!\n"
            "Type 'I am sure' to continue: "
        )
        if i_am_sure != "I am sure":
            raise RuntimeError(f"'{i_am_sure}' is different than 'I am sure'")
    dm.delete(key, delete_from_datastore=delete_from_datastore)


def sync_main(manifest_fname, fast, progress_bar=True):
    dm = DataManifest(manifest_fname)
    dm.sync(fast=fast, progress_bar=progress_bar)


def checkout_main(manifest_fname, checkout_dir, sync=False, fast=False, progress_bar=True):
    if os.path.exists(checkout_dir):
        raise ValueError(
            f"Checkout directory '{checkout_dir}' already exists."
            "\nHint: Use sync to update an existing directory."
        )
    dm = DataManifest.checkout(manifest_fname, checkout_prefix=checkout_dir)
    if sync:
        dm.sync(fast=fast, progress_bar=progress_bar)


def parse_args():
    import argparse

    parser = argparse.ArgumentParser(parents=[logging.build_log_parser(),])

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    create_subparser = subparsers.add_parser("create", help="create a new manifest by traversing a directory")
    create_subparser.add_argument("manifest-path", help="Path to data manifest.")
    create_subparser.add_argument("--checkout-prefix", required=True, help="Location of checkout directory.")
    create_subparser.add_argument("--remote-datastore-uri", required=True, help="Location of remote datastore.")
    create_subparser.add_argument("files-and-directories-to-add", nargs='+', help="Files with which to populate data manifest.")
    create_subparser.add_argument(
        "--dry-run", action="store_true", help="Test without doing anything.",
    )
    create_subparser.add_argument("--resume", action="store_true", help="Ignore existing files")

    checkout_subparser = subparsers.add_parser(
        "checkout", help="checkout a new data directory from a manifest"
    )
    checkout_subparser.add_argument("manifest-path", help="Path to data manifest.")
    checkout_subparser.add_argument("--checkout-prefix", required=True, help="Location of checkout directory.")
    checkout_subparser.add_argument("--sync", default=False, action="store_true", help="Sync all remote data")
    checkout_subparser.add_argument("--fast", default=False, action="store_true", help="skip the md5sum check")

    sync_subparser = subparsers.add_parser("sync", help="sync an existing data directory from a manifest")
    sync_subparser.add_argument("manifest-path", help="Path to data manifest.")
    sync_subparser.add_argument("--fast", default=False, action="store_true", help="skip the md5sum check")

    add_subparser = subparsers.add_parser("add", help="add a file to a manifest")
    update_subparser = subparsers.add_parser("update", help="update a record in the manifest")
    for subparser in [add_subparser, update_subparser]:
        subparser.add_argument("manifest-path")
        subparser.add_argument("key", help="key name of the file")
        subparser.add_argument("path", help="path of the file to add")
        subparser.add_argument("--notes", help="Notes to add to the manifest", default="")

    delete_subparser = subparsers.add_parser("delete", help="delete a file from the manifest")
    delete_subparser.add_argument("manifest-path")
    delete_subparser.add_argument("key", help="key name of the file to delete")
    delete_subparser.add_argument(
        "--delete-from-datastore",
        default=False,
        action="store_true",
        help="Delete the file from the datastore.",
    )

    add_subdirectory_subparser = subparsers.add_parser(
        "add-multiple", help="add files to a data manifest by traversing a directory. Keys are inferred from relative file path.",
    )
    add_subdirectory_subparser.add_argument("manifest-path", help="Path to data manifest.")
    add_subdirectory_subparser.add_argument("files-and-directories-to-add", nargs='+', help="Files with which to populate data manifest.")
    add_subdirectory_subparser.add_argument(
        "--dry-run", action="store_true", help="Test without doing anything.",
    )
    add_subdirectory_subparser.add_argument("--resume", action="store_true", help="Ignore existing files")

    args = parser.parse_args()

    logging.configure_root_logger_from_args(args)
    global logger
    logger = logging.getLogger(__name__)

    # if checkout_prefix is supplied then convert to an absolute path
    if (
        hasattr(args, "checkout_prefix")
        and args.checkout_prefix is not None
        and not os.path.isabs(args.checkout_prefix)
    ):
        # if the checkout prefix is not absolute, then make it relative to the manifest
        rel_path = args.checkout_prefix
        abs_manifest_base_path = os.path.dirname(os.path.normpath(os.path.abspath(getattr(args, 'manifest-path'))))
        assert os.path.isabs(abs_manifest_base_path)
        args.checkout_prefix = os.path.normpath(os.path.join(abs_manifest_base_path, rel_path))
        logger.warning(f"Changing the relative checkout prefix path '{rel_path}' to '{args.checkout_prefix}'")

    return args


def main():
    args = parse_args()
    if args.command == "checkout":
        checkout_main(
            getattr(args, 'manifest-path'),
            getattr(args, 'checkout_prefix'),
            args.sync,
            args.fast,
            progress_bar=(not args.quiet),
        )
    elif args.command == "sync":
        sync_main(
            getattr(args, 'manifest-path'), args.fast, progress_bar=(not args.quiet),
        )
    elif args.command == "add":
        add_main(getattr(args, 'manifest-path'), args.key, args.path, args.notes)
    elif args.command == "update":
        update_main(getattr(args, 'manifest-path'), args.key, args.path, args.notes)
    elif args.command == "delete":
        delete_main(getattr(args, 'manifest-path'), args.key, args.delete_from_datastore)
    elif args.command == "add_subdirectory":
        add_subdirectory_main(
            manifest_fname=getattr(args, 'manifest-path'),
            directory_to_add=getattr(args, 'files-and-directories-to-add'),
            checkout_prefix=getattr(args, 'checkout-prefix'),
            resume=args.resume,
            dry_run=args.dry_run,
        )
    elif args.command == "create":
        create_new_manifest_main(
            manifest_fname=getattr(args, 'manifest-path'),
            directory_to_add=getattr(args, 'files-and-directories-to-add'),
            remote_datastore_uri=getattr(args, 'remote_datastore_uri'),
            checkout_prefix=getattr(args, 'checkout_prefix'),
            resume=args.resume,
            dry_run=args.dry_run,
        )
    else:
        assert False, "This should be handled by the arg parser."


if __name__ == "__main__":
    main()
