# from dataclasses import asdict, is_dataclass
# from typing import Any, Optional, Type, Union

# from temporalio import activity, workflow
# from temporalio.worker import (
#     ActivityInboundInterceptor,
#     ExecuteActivityInput,
#     ExecuteWorkflowInput,
#     Interceptor,
#     WorkflowInboundInterceptor,
#     WorkflowInterceptorClassInput,
# )

# # Comment out Sentry imports
# # with workflow.unsafe.imports_passed_through():
# #     from sentry_sdk import Hub, capture_exception, set_context, set_tag

# def _set_common_workflow_tags(info: Union[workflow.Info, activity.Info]):
#     # set_tag("temporal.workflow.type", info.workflow_type)  # Commented out Sentry
#     # set_tag("temporal.workflow.id", info.workflow_id)  # Commented out Sentry
#     pass

# class _SentryActivityInboundInterceptor(ActivityInboundInterceptor):
#     async def execute_activity(self, input: ExecuteActivityInput) -> Any:
#         # with Hub(Hub.current):  # Commented out Sentry
#             # set_tag("temporal.execution_type", "activity")  # Commented out Sentry
#             # set_tag("module", input.fn.__module__ + "." + input.fn.__qualname__)  # Commented out Sentry

#             activity_info = activity.info()
#             _set_common_workflow_tags(activity_info)
#             # set_tag("temporal.activity.id", activity_info.activity_id)  # Commented out Sentry
#             # set_tag("temporal.activity.type", activity_info.activity_type)  # Commented out Sentry
#             # set_tag("temporal.activity.task_queue", activity_info.task_queue)  # Commented out Sentry
#             # set_tag("temporal.workflow.namespace", activity_info.workflow_namespace)  # Commented out Sentry
#             # set_tag("temporal.workflow.run_id", activity_info.workflow_run_id)  # Commented out Sentry
#             try:
#                 return await super().execute_activity(input)
#             except Exception as e:
#                 if len(input.args) == 1 and is_dataclass(input.args[0]):
#                     pass
#                     # set_context("temporal.activity.input", asdict(input.args[0]))  # Commented out Sentry
#                 # set_context("temporal.activity.info", activity.info().__dict__)  # Commented out Sentry
#                 # capture_exception()  # Commented out Sentry
#                 raise e

# class _SentryWorkflowInterceptor(WorkflowInboundInterceptor):
#     async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
#         try:
#             with workflow.unsafe.sandbox_unrestricted():
#                 # with Hub(Hub.current):  # Commented out Sentry
#                     # set_tag("temporal.execution_type", "workflow")  # Commented out Sentry
#                     # set_tag("module", input.run_fn.__module__ + "." + input.run_fn.__qualname__)  # Commented out Sentry
#                     workflow_info = workflow.info()
#                     _set_common_workflow_tags(workflow_info)
#                     # set_tag("temporal.workflow.task_queue", workflow_info.task_queue)  # Commented out Sentry
#                     # set_tag("temporal.workflow.namespace", workflow_info.namespace)  # Commented out Sentry
#                     # set_tag("temporal.workflow.run_id", workflow_info.run_id)  # Commented out Sentry
#                     try:
#                         return await super().execute_workflow(input)
#                     except Exception as e:
#                         if len(input.args) == 1 and is_dataclass(input.args[0]):
#                             pass
#                             # set_context("temporal.workflow.input", asdict(input.args[0]))  # Commented out Sentry
#                         # set_context("temporal.workflow.info", workflow.info().__dict__)  # Commented out Sentry

#                         # if not workflow.unsafe.is_replaying():  # Commented out Sentry
#                         #     with workflow.unsafe.sandbox_unrestricted():
#                         #         capture_exception()  # Commented out Sentry
#                         raise e
#         except Exception as e:
#             return await super().execute_workflow(input)

# class SentryInterceptor(Interceptor):
#     def intercept_activity(
#         self, next: ActivityInboundInterceptor
#     ) -> ActivityInboundInterceptor:
#         return _SentryActivityInboundInterceptor(super().intercept_activity(next))

#     def workflow_interceptor_class(
#         self, input: WorkflowInterceptorClassInput
#     ) -> Optional[Type[WorkflowInboundInterceptor]]:
#         return _SentryWorkflowInterceptor