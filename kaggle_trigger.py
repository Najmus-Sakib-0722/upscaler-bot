import os
import json
import time
import logging
import requests
from kaggle.api.kaggle_api_extended import KaggleApiExtended

logger = logging.getLogger(__name__)

KAGGLE_USERNAME = os.environ["KAGGLE_USERNAME"]
KAGGLE_KERNEL_SLUG = os.environ.get("KAGGLE_KERNEL_SLUG", "image-upscaler-worker")


def get_kaggle_api() -> KaggleApiExtended:
    """Authenticate and return Kaggle API client."""
    api = KaggleApiExtended()
    api.authenticate()
    return api


def trigger_kaggle_job(chat_id: str, file_id: str, status_message_id: str):
    """
    Push a new Kaggle kernel run with environment variables passed as
    kernel metadata so the notebook can read them.

    Strategy: We update the kernel source with the new env vars baked in
    as a tiny JSON config cell, then trigger a new run.
    """
    api = get_kaggle_api()

    # Build the config that the Kaggle notebook will read
    run_config = {
        "GDRIVE_FILE_ID": file_id,
        "TELEGRAM_CHAT_ID": chat_id,
        "TELEGRAM_MESSAGE_ID": status_message_id,
        "TELEGRAM_BOT_TOKEN": os.environ["TELEGRAM_BOT_TOKEN"],
        "GEMINI_API_KEY": os.environ["GEMINI_API_KEY"],
    }

    # Trigger kernel via Kaggle API (kernel push triggers a new run)
    # We use the kernels push endpoint with updated dataset sources
    logger.info(f"Triggering Kaggle kernel for chat_id={chat_id}, file_id={file_id}")

    # Write a tiny trigger dataset to Kaggle datasets that the notebook polls
    # This is the most reliable free-tier method
    _push_config_as_dataset(api, run_config)

    logger.info("Kaggle job triggered successfully")


def _push_config_as_dataset(api: KaggleApiExtended, config: dict):
    """
    Write run config to a Kaggle dataset so the notebook can read it.
    The notebook polls this dataset on startup.
    """
    import tempfile
    import zipfile

    dataset_slug = os.environ.get("KAGGLE_CONFIG_DATASET", "run-config")
    full_slug = f"{KAGGLE_USERNAME}/{dataset_slug}"

    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, "run_config.json")
        with open(config_path, "w") as f:
            json.dump(config, f)

        metadata = {
            "title": dataset_slug,
            "id": full_slug,
            "licenses": [{"name": "CC0-1.0"}],
        }
        meta_path = os.path.join(tmpdir, "dataset-metadata.json")
        with open(meta_path, "w") as f:
            json.dump(metadata, f)

        try:
            api.dataset_create_version(
                folder=tmpdir,
                version_notes=f"run_{int(time.time())}",
                quiet=True,
                convert_to_csv=False,
                delete_old_versions=True,
            )
            logger.info(f"Config dataset updated: {full_slug}")
        except Exception as e:
            logger.warning(f"Dataset version error (may be first run): {e}")
            try:
                api.dataset_create_new(
                    folder=tmpdir,
                    public=False,
                    quiet=True,
                    convert_to_csv=False,
                )
                logger.info(f"Config dataset created: {full_slug}")
            except Exception as e2:
                raise RuntimeError(f"Could not push config dataset: {e2}") from e2

    # Now trigger the kernel run
    _trigger_kernel_run(api)


def _trigger_kernel_run(api: KaggleApiExtended):
    """Trigger a new run of the Kaggle kernel."""
    try:
        response = api.kernel_push(KAGGLE_USERNAME, KAGGLE_KERNEL_SLUG)
        logger.info(f"Kernel run triggered: {response}")
    except Exception as e:
        raise RuntimeError(f"Failed to trigger kernel run: {e}") from e
