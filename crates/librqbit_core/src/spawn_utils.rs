use std::{borrow::Cow, fmt::Display};

use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{Instrument, debug, error, trace};

tokio::task_local! {
    // The tracker of the tasks spawned from the current task, inherited by
    // the tasks it spawns.
    static TASKS: TaskTracker;
}

/// Runs `fut` so that every task spawned through this module while it runs,
/// and every task those tasks spawn, is tracked by `tasks`.
pub async fn tracked<F: std::future::Future>(tasks: TaskTracker, fut: F) -> F::Output {
    TASKS.scope(tasks, fut).await
}

/// Spawns a future with tracing instrumentation, tracked by `tasks`, as are
/// the tasks it spawns.
#[track_caller]
pub fn spawn_in<E: Display + Send + 'static>(
    tasks: &TaskTracker,
    span: tracing::Span,
    name: impl Into<Cow<'static, str>>,
    fut: impl std::future::Future<Output = Result<(), E>> + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    let fut = instrumented(span, name.into(), fut);
    tasks.spawn(TASKS.scope(tasks.clone(), fut))
}

/// Spawns a future with tracing instrumentation. Within [`tracked`] or a
/// tracked task, the new task is tracked by the same tracker.
#[track_caller]
pub fn spawn<E: Display + Send + 'static>(
    span: tracing::Span,
    name: impl Into<Cow<'static, str>>,
    fut: impl std::future::Future<Output = Result<(), E>> + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    if let Ok(tasks) = TASKS.try_with(TaskTracker::clone) {
        return spawn_in(&tasks, span, name, fut);
    }
    tokio::task::spawn(instrumented(span, name.into(), fut))
}

fn instrumented<E: Display + Send + 'static>(
    span: tracing::Span,
    name: Cow<'static, str>,
    fut: impl std::future::Future<Output = Result<(), E>> + Send + 'static,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    async move {
        trace!("started");
        tokio::pin!(fut);
        let mut trace_interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = trace_interval.tick() => {
                    trace!("still running");
                },
                r = &mut fut => {
                    match r {
                        Ok(_) => {
                            trace!("finished");
                        }
                        Err(e) => {
                            error!("{name} finished with error: {:#}", e)
                        }
                    }
                    return;
                }
            }
        }
    }
    .instrument(span)
}

async fn with_cancel<E: Display + Send + 'static>(
    cancellation_token: CancellationToken,
    fut: impl std::future::Future<Output = Result<(), E>> + Send + 'static,
) -> Result<(), E> {
    tokio::select! {
        _ = cancellation_token.cancelled() => {
            debug!("task cancelled");
            Ok(())
        },
        r = fut => r
    }
}

#[track_caller]
pub fn spawn_with_cancel<E: Display + Send + 'static>(
    span: tracing::Span,
    name: impl Into<Cow<'static, str>>,
    cancellation_token: CancellationToken,
    fut: impl std::future::Future<Output = Result<(), E>> + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    spawn(span, name, with_cancel(cancellation_token, fut))
}

/// Like [`spawn_with_cancel`], tracked by `tasks`.
#[track_caller]
pub fn spawn_with_cancel_in<E: Display + Send + 'static>(
    tasks: &TaskTracker,
    span: tracing::Span,
    name: impl Into<Cow<'static, str>>,
    cancellation_token: CancellationToken,
    fut: impl std::future::Future<Output = Result<(), E>> + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    spawn_in(tasks, span, name, with_cancel(cancellation_token, fut))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn tasks_spawned_in_a_tracked_scope_and_their_children_are_tracked() {
        let tasks = TaskTracker::new();
        let token = CancellationToken::new();
        tracked(tasks.clone(), async {
            let token = token.clone();
            spawn(debug_span(), "parent", async move {
                spawn_with_cancel(
                    debug_span(),
                    "child",
                    token,
                    std::future::pending::<Result<(), std::convert::Infallible>>(),
                )
                .await
                .ok();
                Ok::<(), std::convert::Infallible>(())
            });
        })
        .await;
        // The scope spawned the parent, and the parent the child.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(tasks.len(), 2);
        tasks.close();
        token.cancel();
        tokio::time::timeout(Duration::from_secs(5), tasks.wait())
            .await
            .expect("canceling ends every tracked task");
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn tasks_outside_a_tracked_scope_stay_untracked() {
        let tasks = TaskTracker::new();
        let token = CancellationToken::new();
        let untracked = spawn_with_cancel(
            debug_span(),
            "untracked",
            token.clone(),
            std::future::pending::<Result<(), std::convert::Infallible>>(),
        );
        tracked(tasks.clone(), async {}).await;
        assert!(tasks.is_empty());
        token.cancel();
        untracked.await.ok();
    }

    fn debug_span() -> tracing::Span {
        tracing::debug_span!("test")
    }
}
