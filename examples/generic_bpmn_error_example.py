import logging
from concurrent.futures.thread import ThreadPoolExecutor

import time
from frozendict import frozendict

from camunda.external_task.external_task import ExternalTask
from camunda.external_task.external_task_worker import ExternalTaskWorker
from camunda.utils.log_utils import log_with_context

logger = logging.getLogger(__name__)

default_config = frozendict({
    "maxTasks": 1,
    "lockDuration": 10000,
    "asyncResponseTimeout": 30000,
    "retries": 3,
    "retryTimeout": 5000,
    "sleepSeconds": 30,
    "isDebug": True,
})


def random_true():
    current_milli_time = int(round(time.time() * 1000))
    return current_milli_time % 2 == 0


def generic_task_handler(task: ExternalTask):
    log_context = frozendict({"WORKER_ID": task.get_worker_id(), "TASK_ID": task.get_task_id(),
                              "TOPIC": task.get_topic_name()})
    log_with_context("executing generic_task_handler", log_context)

    if random_true():
        return task.bpmn_error("reject", "FRAUD")
    elif random_true():
        return task.complete({})
    else:
        return task.failure("Task failed", "Task failed", 0, default_config.get("retryTimeout"))


def task_complete_success(task: ExternalTask):
    return task.complete({})


def raise_reject_error(task: ExternalTask):
    return task.bpmn_error("reject", "FRAUD")


def close_application(task: ExternalTask):
    return task.bpmn_error("close", "Product not accepted by user")


def trigger_manual_review(task: ExternalTask):
    error_task_id = task._context["activityId"]
    return task.bpmn_error("manual_review", "Moving to Manual Review",
                           {"errorTaskId": error_task_id,
                            "reason": f"Manual review needed for {error_task_id} reason"})


def continue_next_task(task: ExternalTask):
    error_task_id = task.get_variable("errorTaskId")
    result = "continue"
    log_with_context(f"updating manual review task with result = {result}, error_task_id = {error_task_id}")
    return task.complete({"result": result})


def retry_task(task: ExternalTask):
    error_task_id = task.get_variable("errorTaskId")
    result = "retry"
    log_with_context(f"updating manual review task with result = {result}, error_task_id = {error_task_id}")
    return task.complete({"result": result})


def move_to_needs_info(task: ExternalTask):
    var_error_task_id = task.get_variable("errorTaskId")
    activity_error_task_id = task._context["activityId"]
    error_task_id = var_error_task_id if var_error_task_id else activity_error_task_id
    log_with_context(f"moving to needs_info: activity_error_task_id = {activity_error_task_id}, "
                     f"var_error_task_id = {var_error_task_id}, using error_task_id = {error_task_id}")
    return task.bpmn_error("needs_info", "moving to needs info for possible fraud",
                           {"errorTaskId": error_task_id, "add_bank": True, "identity_docs": False, "student_id": True})


def main():
    configure_logging()
    # ----------------------------------------------------------------------------------------------------------------
    # Scenario: Manual review triggered by XPN task and then Manual review task raises reject error.
    # Reject task should get triggered
    # http://localhost:8080/engine-rest/history/activity-instance?processInstanceId=d7a9d760-11ef-11eb-8a45-0242ac130003&sortBy=startTime&sortOrder=asc
    # topics = [
    #     (["REQUEST_IDV_DATA", "UPDATE_IDV_DATA",
    #       "REQUEST_CAPTURE_DATA", "UPDATE_CAPTURE_DATA",
    #       "REQUEST_CREDIT_PULL_CONSENT", "UPDATE_CREDIT_PULL_CONSENT"], task_complete_success),
    #     ("XPN_CREDIT_PULL", trigger_manual_review),
    #     ("REQUEST_MANUAL_REVIEW", task_complete_success),
    #     ("UPDATE_MANUAL_REVIEW", raise_reject_error),
    # ]
    # ----------------------------------------------------------------------------------------------------------------
    # Scenario: Manual review triggered by XPN task and then Manual review task results in "continue",
    # this should result in continuing the task after XPN Task
    # {{CAMUNDA_REST_URL}}/history/activity-instance?processInstanceId=da134b1b-11f1-11eb-8a45-0242ac130003&sortBy=startTime&sortOrder=asc
    # topics = [
    #     (["REQUEST_IDV_DATA", "UPDATE_IDV_DATA",
    #       "REQUEST_CAPTURE_DATA", "UPDATE_CAPTURE_DATA",
    #       "REQUEST_CREDIT_PULL_CONSENT", "UPDATE_CREDIT_PULL_CONSENT"], task_complete_success),
    #     ("XPN_CREDIT_PULL", trigger_manual_review),
    #     ("REQUEST_MANUAL_REVIEW", task_complete_success),
    #     ("UPDATE_MANUAL_REVIEW", continue_next_task),
    # ]
    # ----------------------------------------------------------------------------------------------------------------
    # Scenario: Manual review triggered by XPN task and then workflow waits at Update Manual review task.
    # REJECT_APPLICATION message is sent to reject the application.
    # It should invoke the Reject application process, but instead getting error:
    # org.camunda.bpm.engine.MismatchingMessageCorrelationException: Cannot correlate message 'REJECT_APPLICATION': No process definition or execution matches the parameters
    # topics = [
    #     (["REQUEST_IDV_DATA", "UPDATE_IDV_DATA",
    #       "REQUEST_CAPTURE_DATA", "UPDATE_CAPTURE_DATA",
    #       "REQUEST_CREDIT_PULL_CONSENT", "UPDATE_CREDIT_PULL_CONSENT"], task_complete_success),
    #     ("XPN_CREDIT_PULL", trigger_manual_review),
    #     ("REQUEST_MANUAL_REVIEW", task_complete_success),
    # ]
    # ----------------------------------------------------------------------------------------------------------------
    # Scenario: Update product acceptance raises "close" error to close the application
    # topics = [
    #     (["REQUEST_IDV_DATA", "UPDATE_IDV_DATA",
    #       "REQUEST_CAPTURE_DATA", "UPDATE_CAPTURE_DATA",
    #       "REQUEST_CREDIT_PULL_CONSENT", "UPDATE_CREDIT_PULL_CONSENT"], task_complete_success),
    #     ("XPN_CREDIT_PULL", trigger_manual_review),
    # ]
    # ----------------------------------------------------------------------------------------------------------------
    topics = [
        ("STEP_1", task_complete_success),
        ("STEP_2", trigger_manual_review),
        ("REQUEST_MANUAL_REVIEW", task_complete_success),
    ]
    # ----------------------------------------------------------------------------------------------------------------
    # ("REQUEST_NEEDS_INFO", task_complete_success),
    # ("REQUEST_ADD_BANK", task_complete_success),
    # ("UPDATE_ADD_BANK", task_complete_success),
    # ("REQUEST_IDENTITY_DOCS", task_complete_success),
    # ("UPDATE_IDENTITY_DOCS", task_complete_success),
    # ("REQUEST_STUDENT_ID", task_complete_success),
    # ("UPDATE_STUDENT_ID", task_complete_success),
    # ("UPDATE_MANUAL_REVIEW", continue_next_task),
    # ("UPDATE_MANUAL_REVIEW", retry_task),
    # ("SCORING", trigger_manual_review),
    # ("REQUEST_PRODUCT_ACCEPTANCE", raise_reject_error),
    # ("UPDATE_PRODUCT_ACCEPTANCE", raise_reject_error),
    # ("CREATE_PLATFORM_USER", raise_reject_error),
    # ("REJECT_APPLICATION", reject_application),
    executor = ThreadPoolExecutor(max_workers=len(topics))
    for index, topic_handler in enumerate(topics):
        topic = topic_handler[0]
        handler_func = topic_handler[1]
        executor.submit(ExternalTaskWorker(worker_id=index, config=default_config).subscribe, topic, handler_func)


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])


if __name__ == '__main__':
    main()
