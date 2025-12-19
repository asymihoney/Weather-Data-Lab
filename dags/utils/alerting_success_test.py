from airflow.utils.email import send_email

# this is a success alert function for warehouse_dag, uncomment if needed
def task_success_alert(context):
    task = context["task_instance"]
    dag_id = task.dag_id
    task_id = task.task_id
    execution_date = context["execution_date"]

    send_email(
        to=["dsforasmi1@gmail.com"],
        subject=f"SUCCESS: {dag_id}.{task_id}",
        html_content=f"""
        <p>Task succeeded</p>
        <ul>
          <li>DAG: {dag_id}</li>
          <li>Task: {task_id}</li>
          <li>Execution date: {execution_date}</li>
        </ul>
        """
    )
