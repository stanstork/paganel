//! Tests for block structure parsing

use ppl_syntax::builder::parse;

#[test]
fn test_parse_simple_define() {
    let input = r#"
        define {
            tax_rate = 1.4
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    assert!(doc.define_block.is_some());

    let define = doc.define_block.unwrap();
    assert_eq!(define.attributes.len(), 1);
    assert_eq!(define.attributes[0].key.name, "tax_rate");
}

#[test]
fn test_parse_execution_block() {
    let input = r#"
        execution {
            strategy = "parallel"
            max_concurrency = 8
            on_failure = "continue"
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    assert!(doc.execution_block.is_some());

    let execution = doc.execution_block.unwrap();
    assert_eq!(execution.attributes.len(), 3);
    assert_eq!(execution.attributes[0].key.name, "strategy");
    assert_eq!(execution.attributes[1].key.name, "max_concurrency");
    assert_eq!(execution.attributes[2].key.name, "on_failure");
}

#[test]
fn test_parse_execution_block_with_timeouts() {
    let input = r#"
        execution {
            strategy = "parallel"
            max_concurrency = 4
            on_failure = "fail_fast"
            pipeline_timeout = "30m"
            total_timeout = "4h"
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    assert!(doc.execution_block.is_some());

    let execution = doc.execution_block.unwrap();
    assert_eq!(execution.attributes.len(), 5);
}

#[test]
fn test_parse_connection() {
    let input = r#"
        connection "mysql_prod" {
            driver = "mysql"
            url = "localhost:3306"
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    assert_eq!(doc.connections.len(), 1);

    let conn = &doc.connections[0];
    assert_eq!(conn.name, "mysql_prod");
    assert_eq!(conn.attributes.len(), 2);
}

#[test]
fn test_parse_pipeline() {
    let input = r#"
        connection "db" {
            driver = "mysql"
            url = "localhost"
        }

        pipeline "sync" {
            from {
                connection = connection.db
            }
            to {
                connection = connection.db
            }
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    assert_eq!(doc.connections.len(), 1);
    assert_eq!(doc.pipelines.len(), 1);

    let pipeline = &doc.pipelines[0];
    assert_eq!(pipeline.name, "sync");
    assert!(pipeline.from.is_some());
    assert!(pipeline.to.is_some());
}

#[test]
fn test_non_after_attribute_is_not_read_as_dependencies() {
    // A pipeline-level attribute that is not `after` (here `description`) must
    // not be misread as the DAG dependency list.
    let input = r#"
        pipeline "sync" {
            description = "copy users"
            from { connection = connection.db }
            to { connection = connection.db }
        }
    "#;

    let doc = parse(input).expect("parse should succeed");
    let pipeline = &doc.pipelines[0];
    assert!(
        pipeline.after.is_none(),
        "non-`after` attribute leaked into dependencies: {:?}",
        pipeline.after
    );
}

#[test]
fn test_after_attribute_populates_dependencies() {
    let input = r#"
        pipeline "sync" {
            after = [pipeline.seed]
            from { connection = connection.db }
            to { connection = connection.db }
        }
    "#;

    let doc = parse(input).expect("parse should succeed");
    let pipeline = &doc.pipelines[0];
    assert!(
        pipeline.after.is_some(),
        "`after` attribute did not populate dependencies"
    );
}

#[test]
fn test_parse_validate_block() {
    let input = r#"
        pipeline "sync" {
            from { connection = connection.db }
            to { connection = connection.db }

            validate {
                assert "customer_id_not_null" {
                  check = customers.id is not null
                  message = "Customer ID cannot be null"
                  action = skip
                }

                warn "missing_email" {
                  check = customers.email is not null
                  message = "Customer email is missing"
                }
            }

        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let pipeline = &doc.pipelines[0];
    assert!(pipeline.validate_block.is_some());

    let validate = pipeline.validate_block.as_ref().unwrap();
    assert_eq!(validate.checks.len(), 2);
}

#[test]
fn test_parse_nested_blocks() {
    let input = r#"
        connection "db" {
            driver = "mysql"
            url = "localhost"

            pool {
                max_size = 20
                min_size = 5
            }

            retry {
                max_attempts = 3
            }
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let conn = &doc.connections[0];
    assert_eq!(conn.nested_blocks.len(), 2);
}

#[test]
fn test_parse_join_clause() {
    let input = r#"
        pipeline "sync" {
            from { connection = connection.db }
            to { connection = connection.db }

            with {
              orders from orders where customers.id == orders.customer_id
              products from products where orders.product_id == products.id
            }
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let pipeline = &doc.pipelines[0];
    assert!(pipeline.with_block.is_some());

    let with_block = pipeline.with_block.as_ref().unwrap();
    assert_eq!(with_block.joins.len(), 2);
}

#[test]
fn test_parse_on_error_block() {
    let input = r#"
        pipeline "sync" {
            from { connection = connection.db }
            to { connection = connection.db }

            on_error {
                retry {
                    max_attempts = 3
                    backoff = "exponential"
                }

                failed_rows {
                    table = "failed_rows"
                }
            }
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let pipeline = &doc.pipelines[0];
    assert!(pipeline.on_error_block.is_some());

    let on_error = pipeline.on_error_block.as_ref().unwrap();
    assert!(on_error.retry.is_some());
    assert!(on_error.failed_rows.is_some());
}

#[test]
fn test_parse_with_comments() {
    let input = r#"
        // This is a comment
        define {
            tax_rate = 1.4  // inline comment
        }

        /* Block comment
           spanning multiple lines */
        connection "db" {
            driver = "mysql"
            url = "localhost"
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
}

#[test]
fn test_build_plugin_block_minimal() {
    let input = r#"
        plugin "fraud" {
            path = "./plugins/fraud.wasm"
        }
    "#;
    let doc = parse(input).expect("should parse");
    assert_eq!(doc.plugins.len(), 1);
    let p = &doc.plugins[0];
    assert_eq!(p.name, "fraud");
    assert_eq!(p.attributes.len(), 1);
    assert_eq!(p.attributes[0].key.name, "path");
    assert!(p.nested_blocks.is_empty());
}

#[test]
fn test_build_plugin_block_with_http_host_allowlist() {
    // `allow_http_hosts` is a plain string-array attribute; the grammar must
    // accept it the same way it accepts `allow_fs_read`.
    let input = r#"
        plugin "api_src" {
            path = "./plugins/api.wasm"
            allow_http = true
            allow_http_hosts = ["api.example.com", "api.stripe.com"]
        }
    "#;
    let doc = parse(input).expect("should parse");
    let p = &doc.plugins[0];
    let hosts = p
        .attributes
        .iter()
        .find(|a| a.key.name == "allow_http_hosts")
        .expect("allow_http_hosts attribute present");
    // The value parses as a two-element array expression.
    assert!(
        format!("{:?}", hosts.value).contains("api.stripe.com"),
        "allow_http_hosts should carry both hosts"
    );
}

#[test]
fn test_build_plugin_block_with_config_nested() {
    let input = r#"
        plugin "stripe_src" {
            path = "./plugins/stripe.wasm"
            allow_http = true
            config {
                base_url = "https://api.stripe.com"
                api_key  = "sk_test_xyz"
            }
        }
    "#;
    let doc = parse(input).expect("should parse");
    assert_eq!(doc.plugins.len(), 1);
    let p = &doc.plugins[0];
    assert_eq!(p.name, "stripe_src");
    assert_eq!(p.attributes.len(), 2);
    assert_eq!(p.nested_blocks.len(), 1);
    assert_eq!(p.nested_blocks[0].kind, "config");
    assert_eq!(p.nested_blocks[0].attributes.len(), 2);
}

#[test]
fn test_build_multiple_plugins_in_document() {
    let input = r#"
        plugin "a" { path = "./a.wasm" }
        plugin "b" { path = "./b.wasm" allow_http = true }
    "#;
    let doc = parse(input).expect("should parse");
    assert_eq!(doc.plugins.len(), 2);
    assert_eq!(doc.plugins[0].name, "a");
    assert_eq!(doc.plugins[1].name, "b");
}

#[test]
fn test_build_pair_block() {
    let input = r#"
        pair "orders" {
            source {
                connection = connection.app
                table      = "orders"
                where      { status == "paid" }
            }
            target {
                connection = connection.warehouse
                table      = "orders_raw"
                where      { state == "PAID" }
                ignore     = ["_fivetran_synced"]
            }
            map {
                source.customer_id = target.cust_id
                target.amount      = source.total
            }
            key   = ["id"]
            grace = "30s"
        }
    "#;

    let doc = parse(input).expect("parse");
    assert_eq!(doc.pairs.len(), 1);

    let pair = &doc.pairs[0];
    assert_eq!(pair.name, "orders");
    let sides: Vec<&str> = pair.sides.iter().map(|s| s.kind.as_str()).collect();
    assert_eq!(sides, ["source", "target"]);
    assert_eq!(pair.sides[0].where_clauses.len(), 1);
    assert_eq!(pair.sides[1].attributes.len(), 3);

    let map: Vec<(String, String)> = pair
        .map
        .iter()
        .map(|m| (m.left.to_string(), m.right.to_string()))
        .collect();
    assert_eq!(
        map,
        [
            ("source.customer_id".into(), "target.cust_id".into()),
            ("target.amount".into(), "source.total".into()),
        ]
    );
    assert_eq!(pair.attributes.len(), 2);
}

#[test]
fn test_pair_map_line_needs_two_columns() {
    let input = r#"
        pair "orders" {
            source { connection = connection.a table = "t" }
            target { connection = connection.b table = "t" }
            map { source.customer_id = "cust_id" }
        }
    "#;
    assert!(parse(input).is_err());
}

#[test]
fn test_pair_keyword_leaves_identifiers_alone() {
    // A column called `pair` or `pair_id` is still an identifier.
    let input = r#"
        pipeline "p" {
            from { connection = connection.a table = "t" }
            to { connection = connection.b table = "t" }
            select {
                pair    = t.pair
                pair_id = t.pair_id
            }
        }
    "#;

    let doc = parse(input).expect("parse");
    assert!(doc.pairs.is_empty());
    assert_eq!(doc.pipelines.len(), 1);
}
