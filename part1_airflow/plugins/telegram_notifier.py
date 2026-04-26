from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма


def send_telegram_success_message(context): # на вход принимаем словарь с контекстными переменными
    hook = TelegramHook('telegram')
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'text': message
    }) # отправление сообщения


def send_telegram_failure_message(context):
    hook = TelegramHook('telegram')
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = (
        f'Исполнение DAG с id={run_id} прошло неудачно!\n'
        f'Упавшая задача: {task_instance_key_str}'
    )
    hook.send_message({
        'text': message
    }) # отправление сообщения
    