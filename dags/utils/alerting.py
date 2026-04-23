from airflow.utils.email import send_email
from airflow.models import Variable

def task_failure_alert(context):
    task = context["task_instance"]
    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]
    log_url = task.log_url

    subject = f"[AIRFLOW ALERT] Task Failed: {dag_id}.{task.task_id}"

    html_content = f"""
    <h3>Airflow Task Failed</h3>
    <ul>
      <li><b>DAG:</b> {dag_id}</li>
      <li><b>Task:</b> {task.task_id}</li>
      <li><b>Execution Date:</b> {execution_date}</li>
      <li><b>Log:</b> <a href="{log_url}">View Logs</a></li>
    </ul>
    """

    # Mengambil email dari Airflow Variables, jika tidak ada, gunakan default
    alert_email = Variable.get("ALERT_EMAIL", default_var="dsforasmi1@gmail.com")

    send_email(
        to=[alert_email],
        subject=subject,
        html_content=html_content,
    )