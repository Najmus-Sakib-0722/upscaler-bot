import os
import json
import time
import logging
import tempfile
from pathlib import Path

# Kaggle API ইমপোর্ট করার সবচেয়ে নিরাপদ পদ্ধতি
try:
    from kaggle.api.kaggle_api_extended import KaggleApiExtended
except (ImportError, AttributeError):
    try:
        from kaggle import KaggleApi as KaggleApiExtended
    except ImportError:
        raise ImportError("Kaggle library পাওয়া যায়নি। requirements.txt চেক করুন।")

logger = logging.getLogger(__name__)

# পরিবেশ ভেরিয়েবল থেকে তথ্য সংগ্রহ
KAGGLE_USERNAME = os.environ.get("KAGGLE_USERNAME")
KAGGLE_KERNEL_SLUG = os.environ.get("KAGGLE_KERNEL_SLUG", "image-upscaler-worker")
KAGGLE_CONFIG_DATASET = os.environ.get("KAGGLE_CONFIG_DATASET", "run-config")

def get_kaggle_api():
    """Kaggle অ্যাকাউন্টে লগইন করে API ক্লায়েন্ট রিটার্ন করে।"""
    api = KaggleApiExtended()
    api.authenticate()
    return api

def trigger_kaggle_job(chat_id, file_id, status_message_id):
    """
    ১. run_config.json আপডেট করে Kaggle Dataset-এ পাঠায়।
    ২. Kaggle Kernel (GPU) রান করার সিগন্যাল দেয়।
    """
    api = get_kaggle_api()

    # ইউজারের পাঠানো ড্রাইভ লিঙ্ক এবং অন্যান্য তথ্য দিয়ে কনফিগ তৈরি
    run_config = {
        "GDRIVE_FILE_ID": file_id,
        "TELEGRAM_CHAT_ID": chat_id,
        "TELEGRAM_MESSAGE_ID": status_message_id,
        "TELEGRAM_BOT_TOKEN": os.environ.get("TELEGRAM_BOT_TOKEN"),
        "GEMINI_API_KEY": os.environ.get("GEMINI_API_KEY"),
    }

    full_dataset_slug = f"{KAGGLE_USERNAME}/{KAGGLE_CONFIG_DATASET}"
    kernel_full_slug = f"{KAGGLE_USERNAME}/{KAGGLE_KERNEL_SLUG}"

    # একটি অস্থায়ী ফোল্ডারে মেটাডেটা তৈরি (মোবাইলে ফাইল সিস্টেম ঝামেলা এড়াতে)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # run_config.json ফাইল তৈরি
        with open(tmp_path / "run_config.json", "w") as f:
            json.dump(run_config, f)

        # dataset-metadata.json তৈরি
        metadata = {
            "title": KAGGLE_CONFIG_DATASET,
            "id": full_dataset_slug,
            "licenses": [{"name": "CC0-1.0"}]
        }
        with open(tmp_path / "dataset-metadata.json", "w") as f:
            json.dump(metadata, f)

        # ধাপ ১: Dataset আপডেট করা (যাতে Kernel নতুন ফাইল পায়)
        try:
            logger.info(f"Dataset আপডেট হচ্ছে: {full_dataset_slug}")
            api.dataset_create_version(
                folder=tmpdir,
                version_notes=f"Update_{int(time.time())}",
                quiet=True,
                delete_old_versions=True
            )
        except Exception as e:
            logger.warning(f"Dataset আপডেট এরর: {e}. নতুন করে তৈরির চেষ্টা চলছে...")
            try:
                api.dataset_create_new(folder=tmpdir, public=False, quiet=True)
            except Exception as e2:
                raise RuntimeError(f"Dataset আপলোড ব্যর্থ: {str(e2)}")

    # ধাপ ২: Kernel রান করা (GPU Trigger)
    # ৫‌০০ এরর এড়াতে ৩ বার চেষ্টা করার লজিক
    for attempt in range(3):
        try:
            logger.info(f"Kernel রান করার চেষ্টা ({attempt+1}/3)...")
            # এখানে সরাসরি metadata ব্যবহার না করে শুধু push করা হচ্ছে
            api.kernels_push_with_http_info(tmpdir, kernel_full_slug)
            logger.info("✅ Kaggle Kernel সফলভাবে চালু হয়েছে।")
            return True
        except Exception as e:
            if attempt < 2:
                time.sleep(5) # ৫ সেকেন্ড অপেক্ষা করে আবার চেষ্টা
                continue
            else:
                logger.error(f"Kaggle API চূড়ান্ত এরর: {e}")
                raise RuntimeError(f"Kaggle API Error: {str(e)}")
