def get_error_email_templates(**kwargs):
    error_subject = f"DAG {kwargs['dag_id']} failed at task {kwargs['task_id']}"
    if kwargs["exception"]:
        error_email_template = f"""
                                    DAG run: {kwargs['id']} {kwargs['dag_id']} <br>
                                    """
        # Task: {kwargs.task_id} <br>
        # Execution date: {kwargs.execution_date} <br>
        # Run id: {kwargs.run_id} <br>
        # Error: {kwargs.exception_name} <br>
        # Traceback: {formatted_exception}
        return error_subject, error_email_template
    error_email_template = f"""
                                DAG run: {kwargs['id']} {kwargs['dag_id']} <br>
                                """
    # Task: {kwargs.task.task_id} <br>
    # Execution date: {kwargs.dag_run.execution_date} <br>
    # Run id: {kwargs.dag_run.run_id} <br>
    # Error: {kwargs.exception_name} <br>
