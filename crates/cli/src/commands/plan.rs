use crate::{
    Cli,
    commands::{Commands, SampleMethod},
    config,
    error::CliError,
    plan,
    spinner::Spinner,
};
use engine_core::context::env::EnvContext;
use engine_planner::{
    builder::{ReportBuilder, ReportBuilderConfig},
    plan::sample::method::SamplingMethod,
};
use engine_runtime::dag::builder::DagBuilder;
use model::core::value::Value;
use std::{io::IsTerminal, path::Path, sync::Arc};
use tracing::info;

/// Executes the plan command (dry-run migration planning)
pub async fn execute(cli: &Cli, commands: &Commands, env: Arc<EnvContext>) -> Result<(), CliError> {
    if let Commands::Plan {
        config,
        output: output_path,
        json,
        ddl,
        sample,
        sample_size,
        sample_method,
        id_column,
        sample_ids,
        exact_filter: exact_where,
    } = commands
    {
        let config_path = config::resolve_path(config.clone())?;
        info!(config = %config_path, "running dry-run plan");

        // Load core plan
        let core_plan = config::load_plan(&config_path, false, env.clone()).await?;

        // Build DAG
        let dag = build_dag(&core_plan)?;

        // Convert CLI options to planner config. `env` is passed through so
        // `plan --sample` runs plugins with the same `allow_env` grants as apply.
        let plan_config = build_plan_config(
            *sample,
            *sample_size,
            *sample_method,
            id_column.clone(),
            sample_ids.clone(),
            *exact_where,
            env,
        );

        let show_spinner = std::io::stderr().is_terminal()
            && !cli.quiet
            && !*json
            && cli.verbose == 0
            && cli.log_level.is_none();
        let spinner =
            show_spinner.then(|| Spinner::start("Analyzing migration plan…", !cli.no_color));

        let report_builder = ReportBuilder::new(plan_config);
        let report = report_builder
            .build(&core_plan, &dag, Path::new(&config_path))
            .await;

        drop(spinner);
        let report = report?;

        // Render either the human summary (default) or the full JSON document
        // (`--json`, for CI). Color only when writing to a real terminal.
        let to_file = output_path.is_some();
        let rendered = if *json {
            // The full machine-readable report (for CI / tooling).
            serde_json::to_string_pretty(&report)?
        } else {
            let color = !to_file && !cli.no_color && std::io::stdout().is_terminal();
            plan::render(&report, color, *ddl)
        };

        match output_path {
            Some(path) => tokio::fs::write(path, rendered).await?,
            None => {
                if !cli.quiet {
                    print!("{rendered}");
                }
            }
        }
    }

    Ok(())
}

/// Builds the execution DAG from the core plan
fn build_dag(
    core_plan: &engine_core::plan::execution::ExecutionPlan,
) -> Result<engine_runtime::dag::Dag, CliError> {
    let mut dag_builder = DagBuilder::new();

    for pipeline in &core_plan.pipelines {
        dag_builder.add_pipeline(pipeline.name.clone(), pipeline.dependencies.clone())?;
    }

    Ok(dag_builder.build()?)
}

/// Builds report builder configuration from CLI arguments
fn build_plan_config(
    sample: bool,
    sample_size: usize,
    sample_method: SampleMethod,
    id_column: Option<String>,
    sample_ids: Option<Vec<String>>,
    exact_where: bool,
    env: Arc<EnvContext>,
) -> ReportBuilderConfig {
    // Convert CLI sample method to engine SamplingMethod
    let sampling_method = match sample_method {
        SampleMethod::First => SamplingMethod::First,
        SampleMethod::Random => SamplingMethod::Random,
        SampleMethod::Id => SamplingMethod::ById,
    };

    // Convert sample_ids from Vec<String> to Vec<Value>
    let sample_ids_values = sample_ids.map(|ids| {
        ids.into_iter()
            .map(|id| {
                // Try to parse as integer first, otherwise use as string
                if let Ok(num) = id.parse::<i64>() {
                    Value::Int(num)
                } else {
                    Value::String(id)
                }
            })
            .collect()
    });

    ReportBuilderConfig {
        enable_sampling: sample,
        sample_size,
        sample_method: sampling_method,
        id_column: id_column.unwrap_or_default(),
        sample_ids: sample_ids_values,
        exact_where,
        env: Some(env),
        ..Default::default()
    }
}
