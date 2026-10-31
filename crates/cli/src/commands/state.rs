use crate::{
    commands::StateCmd,
    config::{load_plan, resolve_path},
    error::CliError,
    format::{DATE_FORMAT, commas},
};
use engine_core::context::exec::ExecutionContext;
use engine_processing::EnvContext;
use engine_verify::{
    ImportRequest, NoopProgress, VerifyState, connection_target, error::VerifyError, import_table,
};
use model::{
    execution::connection::Connection,
    integrity::import::{import_pipeline, validate_import_name},
};
use std::sync::Arc;

pub async fn run(cmd: &StateCmd, env: Arc<EnvContext>) -> Result<(), CliError> {
    match cmd {
        StateCmd::Import {
            config,
            connection,
            table,
            name,
            key,
        } => {
            import(
                config.clone(),
                connection.as_deref(),
                table,
                name.as_deref(),
                key,
                env.clone(),
            )
            .await
        }
        StateCmd::List { json } => list(*json).await,
        StateCmd::Rm { name } => rm(name).await,
    }
}

/// Hash `table` on a connection from the config and store it as a baseline.
async fn import(
    config_path: Option<String>,
    connection: Option<&str>,
    table: &str,
    name: Option<&str>,
    key: &[String],
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    let config_path = resolve_path(config_path)?;
    let plan = load_plan(&config_path, false, env.clone()).await?;

    let conn = find_connection(&plan.connections, connection, &config_path)?;
    let name = import_name(name, table)?;

    if plan.pipelines.iter().any(|p| p.name == name) {
        return Err(CliError::UserMessage(format!(
            "'{name}' is a pipeline in {config_path}; pass a different --name"
        )));
    }

    let state = VerifyState::open_default()?;
    let ctx = ExecutionContext::new(&plan, state.store.clone(), state.hash_log.clone(), env);
    let driver = ctx.resolve_driver(conn).await.map_err(VerifyError::from)?;

    let req = ImportRequest {
        name,
        connection: conn.name.clone(),
        target: conn
            .properties
            .get_string("url")
            .as_deref()
            .and_then(connection_target),
        table: table.to_string(),
        key_override: (!key.is_empty()).then(|| key.to_vec()),
    };
    let (receipt, record) = import_table(&driver, &req, &state, &mut NoopProgress).await?;

    println!(
        "imported {} as '{}' ({}): {} rows, root {}",
        record.table,
        record.name,
        record.connection,
        commas(record.rows),
        receipt.root_hex()
    );
    println!("check it with: pag drift -c {config_path}");

    Ok(())
}

async fn list(json: bool) -> Result<(), CliError> {
    let state = VerifyState::open_default()?;
    let imports = state
        .store
        .list_imports()
        .await
        .map_err(VerifyError::from)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&imports)?);
        return Ok(());
    }

    if imports.is_empty() {
        println!("No imports (add one with `pag state import`).");
        return Ok(());
    }

    for r in &imports {
        println!(
            "{}  {}/{}  {} rows  imported {}",
            r.name,
            r.connection,
            r.table,
            commas(r.rows),
            r.imported_at.format(DATE_FORMAT)
        );
    }

    Ok(())
}

async fn rm(name: &str) -> Result<(), CliError> {
    let state = VerifyState::open_default()?;

    if !state
        .store
        .delete_import(name)
        .await
        .map_err(VerifyError::from)?
    {
        let names = state
            .store
            .list_imports()
            .await
            .map_err(VerifyError::from)?
            .into_iter()
            .map(|r| r.name)
            .collect::<Vec<_>>();

        return Err(CliError::UserMessage(if names.is_empty() {
            format!("no import named '{name}'; there are no imports")
        } else {
            format!("no import named '{name}'; imports: {}", names.join(", "))
        }));
    }

    state
        .hash_log
        .clear_pipeline(&import_pipeline(name))
        .map_err(VerifyError::from)?;

    println!("removed import '{name}'");
    Ok(())
}

/// The connection to import from: the one named, or the config's only one.
fn find_connection<'a>(
    connections: &'a [Connection],
    name: Option<&str>,
    config_path: &str,
) -> Result<&'a Connection, CliError> {
    let defined = || {
        connections
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };

    match name {
        Some(name) => connections.iter().find(|c| c.name == name).ok_or_else(|| {
            CliError::UserMessage(format!(
                "no connection '{name}' in {config_path}; defined: {}",
                defined()
            ))
        }),
        None => match connections {
            [only] => Ok(only),
            [] => Err(CliError::UserMessage(format!(
                "{config_path} defines no connection; add a connection block for the database to import from"
            ))),
            _ => Err(CliError::UserMessage(format!(
                "{config_path} defines several connections ({}); pass --connection",
                defined()
            ))),
        },
    }
}

/// `--name`, or the table name when that is a valid import name.
fn import_name(name: Option<&str>, table: &str) -> Result<String, CliError> {
    match name {
        Some(name) => {
            validate_import_name(name).map_err(CliError::UserMessage)?;
            Ok(name.to_string())
        }
        None => {
            validate_import_name(table).map_err(|_| {
                CliError::UserMessage(format!(
                    "table '{table}' isn't a valid import name; pass --name"
                ))
            })?;
            Ok(table.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connection(name: &str) -> Connection {
        Connection {
            name: name.into(),
            driver: "postgres".into(),
            properties: Default::default(),
            nested_configs: Default::default(),
        }
    }

    fn connections(names: &[&str]) -> Vec<Connection> {
        names.iter().map(|n| connection(n)).collect()
    }

    fn message(err: CliError) -> String {
        match err {
            CliError::UserMessage(msg) => msg,
            other => panic!("expected a user message, got {other:?}"),
        }
    }

    #[test]
    fn the_only_connection_needs_no_flag() {
        let all = connections(&["prod"]);
        let conn = find_connection(&all, None, "watch.ppl").unwrap();
        assert_eq!(conn.name, "prod");
    }

    #[test]
    fn several_connections_need_the_flag() {
        let all = connections(&["prod", "replica"]);
        let msg = message(find_connection(&all, None, "watch.ppl").unwrap_err());
        assert!(
            msg.contains("prod, replica") && msg.contains("--connection"),
            "{msg}"
        );

        let conn = find_connection(&all, Some("replica"), "watch.ppl").unwrap();
        assert_eq!(conn.name, "replica");
    }

    #[test]
    fn an_unknown_connection_lists_the_defined_ones() {
        let all = connections(&["prod", "replica"]);
        let msg = message(find_connection(&all, Some("prd"), "watch.ppl").unwrap_err());
        assert_eq!(
            msg,
            "no connection 'prd' in watch.ppl; defined: prod, replica"
        );
    }

    #[test]
    fn the_table_name_is_the_default_import_name() {
        assert_eq!(import_name(None, "orders").unwrap(), "orders");
        assert_eq!(
            import_name(Some("orders_prod"), "orders").unwrap(),
            "orders_prod"
        );

        let msg = message(import_name(None, "sales.orders").unwrap_err());
        assert_eq!(
            msg,
            "table 'sales.orders' isn't a valid import name; pass --name"
        );
        assert!(import_name(Some("bad name"), "orders").is_err());
    }
}
