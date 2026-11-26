from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_id, extract_video_data, save_to_json

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "personal_project",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "test@abc.com",
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz)
}


with DAG(
    dag_id = 'produce_json',
    default_args = default_args,
    description='Dag to produce raw yt api data in json',
    schedule = '0 14 * * *',
    catchup=False
)as dag:

    playlist_id = get_playlist_id()
    video_ids = get_video_id(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    playlist_id >> video_ids >> extract_data >> save_to_json_task