import os
import json
import time
import logging
import tempfile
import requests

# Kaggle API ইমপোর্ট করার নিরাপদ পদ্ধতি
try:
    from kaggle.api.kaggle_api_extended import KaggleApiExtended
except (ImportError, AttributeError):
    try:
        from kaggle import KaggleApi as KaggleApiExtended
    except ImportError:
        raise ImportError("Kaggle library not found. Please check requirements.txt")

logger = logging.getLogger(__name__)

KAGGLE_USERNAME = os.environ["KAGGLE_USERNAME"]
KAGGLE_KERNEL_SLUG = os.environ.get("KAGGLE_KERNEL_SLUG", "image-upscaler-worker")

def get_kaggle_api():
    """Authenticate and return Kaggle API client."""
    api = KaggleApiExtended()
    api.authenticate()
    return api

def trigger_kaggle_job(chat_id, file_id, status_message_id):
    api = get_kaggle_api()

    run_config = {
        "GDRIVE_FILE_ID": file_id,
        "TELEGRAM_CHAT_ID": chat_id,
        "TELEGRAM_MESSAGE_ID": status_message_id,
        "TELEGRAM_BOT_TOKEN": os.environ["TELEGRAM_BOT_TOKEN"],
        "GEMINI_API_KEY": os.environ["GEMINI_API_KEY"],
    }

    dataset_slug = os.environ.get("KAGGLE_CONFIG_DATASET", "run-config")
    full_slug = f"{KAGGLE_USERNAME}/{dataset_slug}"

    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, "run_config.json")
        with open(config_path, "w") as f:
            json.dump(run_config, f)

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
            logger.warning(f"Dataset version error: {e}")
            try:
                api.dataset_create_new(
                    folder=tmpdir,
                    public=False,
                    quiet=True,
                    convert_to_csv=False,
                )
            except Exception as e2:
                raise RuntimeError(f"Could not push config dataset: {e2}")

    # Trigger kernel run
    try:
        api.kernel_push_by_id(f"{KAGGLE_USERNAME}/{KAGGLE_KERNEL_SLUG}")
        logger.info("Kernel run triggered successfully.")
    except Exception as e:
        # Fallback to general push
        logger.error(f"Kernel trigger error: {e}")
        raise e
