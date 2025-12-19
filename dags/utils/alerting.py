from airflow.utils.email import send_email

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

    send_email(
        to=["dsforasmi1@gmail.com"],
        subject=subject,
        html_content=html_content,
    )