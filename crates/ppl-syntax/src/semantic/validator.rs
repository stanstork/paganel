use crate::{
    ast::{
        attribute::Attribute,
        block::{ConnectionBlock, DefineBlock, HookBlock, PairBlock},
        doc::PplDocument,
        expr::{Expression, ExpressionKind},
        pipeline::{FromBlock, PipelineBlock, ToBlock},
    },
    errors::{ValidationIssue, ValidationIssueKind, ValidationResult},
    semantic::symbol_table::SymbolTable,
};
use std::collections::{HashMap, HashSet};

/// Semantic validator for PPL documents
pub struct SemanticValidator {
    symbols: SymbolTable,
    issues: ValidationResult,
}

impl SemanticValidator {
    pub fn new() -> Self {
        SemanticValidator {
            symbols: SymbolTable::new(),
            issues: ValidationResult::new(),
        }
    }

    pub fn validate(&mut self, document: &PplDocument) -> ValidationResult {
        // Build symbol table and check for duplicates
        self.build_symbol_table(document);

        // Validate document semantics
        self.validate_document(document);

        // Check for circular dependencies
        self.check_circular_dependencies(document);

        // Check for unused declarations (warnings)
        self.check_unused_declarations();

        self.issues.clone()
    }

    fn build_symbol_table(&mut self, document: &PplDocument) {
        // Register define constants
        if let Some(ref define) = document.define_block {
            for attr in &define.attributes {
                if let Some(first_span) = self
                    .symbols
                    .add_define_constant(attr.key.name.clone(), attr.key.span)
                {
                    self.issues.add_error(ValidationIssue::error(
                        ValidationIssueKind::DuplicateDefineAttribute {
                            name: attr.key.name.clone(),
                            first_location: first_span,
                        },
                        attr.key.span,
                    ));
                }
            }
        }

        // Register connections
        for conn in &document.connections {
            if let Some(first_span) = self.symbols.add_connection(conn.name.clone(), conn.span) {
                self.issues.add_error(ValidationIssue::error(
                    ValidationIssueKind::DuplicateConnection {
                        name: conn.name.clone(),
                        first_location: first_span,
                    },
                    conn.span,
                ));
            }
        }

        // Register plugins
        for plugin in &document.plugins {
            if let Some(first_span) = self.symbols.add_plugin(plugin.name.clone(), plugin.span) {
                self.issues.add_error(ValidationIssue::error(
                    ValidationIssueKind::DuplicatePlugin {
                        name: plugin.name.clone(),
                        first_location: first_span,
                    },
                    plugin.span,
                ));
            }
        }

        // Register pipelines
        for pipeline in &document.pipelines {
            if let Some(first_span) = self
                .symbols
                .add_pipeline(pipeline.name.clone(), pipeline.span)
            {
                self.issues.add_error(ValidationIssue::error(
                    ValidationIssueKind::DuplicatePipeline {
                        name: pipeline.name.clone(),
                        first_location: first_span,
                    },
                    pipeline.span,
                ));
            }
        }
    }

    fn validate_document(&mut self, document: &PplDocument) {
        if let Some(ref define) = document.define_block {
            self.validate_define_block(define);
        }

        for conn in &document.connections {
            self.validate_connection_block(conn);
        }

        for pipeline in &document.pipelines {
            self.validate_pipeline_block(pipeline);
        }

        for pair in &document.pairs {
            self.validate_pair_block(pair);
        }

        for hook in &document.hooks {
            self.validate_hook_block(hook);
        }
    }

    fn validate_define_block(&mut self, block: &DefineBlock) {
        if block.attributes.is_empty() {
            self.issues.add_warning(ValidationIssue::warning(
                ValidationIssueKind::EmptyBlock {
                    block_type: "define".to_string(),
                },
                block.span,
            ));
        }

        // Validate each attribute's value expression
        for attr in &block.attributes {
            self.validate_expression(&attr.value);
        }
    }

    fn validate_connection_block(&mut self, block: &ConnectionBlock) {
        // Check required fields
        let has_driver = block
            .attributes
            .iter()
            .any(|attr| attr.key.name == "driver");
        let has_url = block.attributes.iter().any(|attr| attr.key.name == "url");

        if !has_driver {
            self.issues.add_error(ValidationIssue::error(
                ValidationIssueKind::MissingRequiredField {
                    block_type: "connection".to_string(),
                    field: "driver".to_string(),
                },
                block.span,
            ));
        }

        if !has_url {
            self.issues.add_error(ValidationIssue::error(
                ValidationIssueKind::MissingRequiredField {
                    block_type: "connection".to_string(),
                    field: "url".to_string(),
                },
                block.span,
            ));
        }

        // Validate each attribute's value expression
        for attr in &block.attributes {
            self.validate_expression(&attr.value);
        }

        // Validate nested blocks
        for nested in &block.nested_blocks {
            for attr in &nested.attributes {
                self.validate_expression(&attr.value);
            }
        }
    }

    fn validate_pipeline_block(&mut self, block: &PipelineBlock) {
        // Check required blocks
        if block.from.is_none() {
            self.issues.add_error(ValidationIssue::error(
                ValidationIssueKind::MissingRequiredField {
                    block_type: "pipeline".to_string(),
                    field: "from".to_string(),
                },
                block.span,
            ));
        }

        if block.to.is_none() {
            self.issues.add_error(ValidationIssue::error(
                ValidationIssueKind::MissingRequiredField {
                    block_type: "pipeline".to_string(),
                    field: "to".to_string(),
                },
                block.span,
            ));
        }

        if let Some(after_deps) = &block.after {
            for dep_expr in after_deps {
                self.validate_expression(dep_expr);

                // Check if it's a pipeline reference
                if let ExpressionKind::DotNotation(path) = &dep_expr.kind
                    && path.segments.len() == 2
                    && path.segments[0] == "pipeline"
                {
                    let pipeline_name = &path.segments[1];
                    self.symbols.mark_pipeline_used(pipeline_name);

                    if !self.symbols.pipelines.contains_key(pipeline_name) {
                        self.issues.add_error(ValidationIssue::error(
                            ValidationIssueKind::UndefinedPipeline {
                                name: pipeline_name.clone(),
                            },
                            dep_expr.span,
                        ));
                    }
                }
            }
        }

        if let Some(from_block) = &block.from {
            self.validate_from_block(from_block);
        }

        if let Some(to_block) = &block.to {
            self.validate_to_block(to_block);
        }

        for where_clause in &block.where_clauses {
            for condition in &where_clause.conditions {
                self.validate_expression(condition);
            }
        }

        if let Some(with) = &block.with_block {
            for join in &with.joins {
                if let Some(condition) = &join.condition {
                    self.validate_expression(condition);
                }
            }
        }

        if let Some(select) = &block.select_block {
            for field in &select.fields {
                self.validate_expression(&field.value);
            }
        }

        if let Some(validate) = &block.validate_block {
            for check in &validate.checks {
                self.validate_expression(&check.body.check);
            }
        }

        if let Some(on_error) = &block.on_error_block {
            if let Some(retry) = &on_error.retry {
                for attr in &retry.attributes {
                    self.validate_expression(&attr.value);
                }
            }
            if let Some(failed_rows) = &on_error.failed_rows {
                for attr in &failed_rows.attributes {
                    self.validate_expression(&attr.value);
                }
            }
        }

        if let Some(paginate) = &block.paginate_block {
            for attr in &paginate.attributes {
                self.validate_expression(&attr.value);
            }
        }

        if let Some(settings) = &block.settings_block {
            for attr in &settings.attributes {
                self.validate_expression(&attr.value);
            }
        }
    }

    fn validate_pair_block(&mut self, block: &PairBlock) {
        for side in ["source", "target"] {
            let Some(block_side) = block.sides.iter().find(|s| s.kind == side) else {
                self.issues.add_error(ValidationIssue::error(
                    ValidationIssueKind::MissingRequiredField {
                        block_type: "pair".to_string(),
                        field: side.to_string(),
                    },
                    block.span,
                ));
                continue;
            };

            for attr in &block_side.attributes {
                self.validate_expression(&attr.value);
                if attr.key.name == "connection" {
                    self.validate_endpoint_ref(&attr.value);
                }
            }
        }
    }

    fn validate_hook_block(&mut self, block: &HookBlock) {
        let nested = block.nested_blocks.iter().flat_map(|b| &b.attributes);
        for attr in block.attributes.iter().chain(nested) {
            self.validate_expression(&attr.value);
        }
    }

    fn validate_expression(&mut self, expr: &Expression) {
        match &expr.kind {
            ExpressionKind::DotNotation(path) => {
                // Check for define.* references
                if path.segments.len() == 2 && path.segments[0] == "define" {
                    let const_name = &path.segments[1];
                    self.symbols.mark_define_constant_used(const_name);

                    if !self.symbols.define_constants.contains_key(const_name) {
                        self.issues.add_error(ValidationIssue::error(
                            ValidationIssueKind::UndefinedDefineConstant {
                                name: const_name.clone(),
                            },
                            expr.span,
                        ));
                    }
                }
                // connection.* and pipeline.* references are checked in context
            }
            ExpressionKind::Binary { left, right, .. } => {
                self.validate_expression(left);
                self.validate_expression(right);
            }
            ExpressionKind::Unary { operand, .. } => {
                self.validate_expression(operand);
            }
            ExpressionKind::FunctionCall { arguments, .. } => {
                for arg in arguments {
                    self.validate_expression(arg);
                }
            }
            ExpressionKind::Array(elements) => {
                for elem in elements {
                    self.validate_expression(elem);
                }
            }
            ExpressionKind::WhenExpression {
                branches,
                else_value,
            } => {
                for branch in branches {
                    self.validate_expression(&branch.condition);
                    self.validate_expression(&branch.value);
                }
                if let Some(else_expr) = else_value {
                    self.validate_expression(else_expr);
                }
            }
            ExpressionKind::IsNull(operand) | ExpressionKind::IsNotNull(operand) => {
                self.validate_expression(operand);
            }
            ExpressionKind::PluginCall(call) => {
                if !self.symbols.plugins.contains_key(&call.plugin_name) {
                    self.issues.add_error(ValidationIssue::error(
                        ValidationIssueKind::UndefinedPlugin {
                            name: call.plugin_name.clone(),
                        },
                        call.span,
                    ));
                }
                // input fields' source_refs are bare table.column DotPaths;
                // those don't go through the symbol table.
            }
            ExpressionKind::Grouped(inner) => {
                self.validate_expression(inner);
            }
            _ => {} // Literals and identifiers are always valid
        }
    }

    fn validate_from_block(&mut self, block: &FromBlock) {
        let has_connection = block
            .attributes
            .iter()
            .any(|attr| attr.key.name == "connection");

        if !has_connection {
            self.issues.add_error(ValidationIssue::error(
                ValidationIssueKind::MissingRequiredField {
                    block_type: "from".to_string(),
                    field: "connection".to_string(),
                },
                block.span,
            ));
        }

        for attr in &block.attributes {
            self.validate_expression(&attr.value);
            if attr.key.name == "connection" {
                self.validate_endpoint_ref(&attr.value);
            }
        }
    }

    fn validate_to_block(&mut self, block: &ToBlock) {
        let has_connection = block
            .attributes
            .iter()
            .any(|attr| attr.key.name == "connection");

        if !has_connection {
            self.issues.add_error(ValidationIssue::error(
                ValidationIssueKind::MissingRequiredField {
                    block_type: "to".to_string(),
                    field: "connection".to_string(),
                },
                block.span,
            ));
        }

        for attr in &block.attributes {
            self.validate_expression(&attr.value);
            if attr.key.name == "connection" {
                self.validate_endpoint_ref(&attr.value);
            }
        }
    }

    /// Validate `connection = connection.<name>` or `connection = plugin.<name>`
    /// in a `from`/`to` block, marking the symbol used and reporting any
    /// reference to an undeclared connection or plugin.
    fn validate_endpoint_ref(&mut self, value: &Expression) {
        let ExpressionKind::DotNotation(ref path) = value.kind else {
            return;
        };
        if path.segments.len() != 2 {
            return;
        }
        let name = &path.segments[1];
        match path.segments[0].as_str() {
            "connection" => {
                self.symbols.mark_connection_used(name);
                if !self.symbols.connections.contains_key(name) {
                    self.issues.add_error(ValidationIssue::error(
                        ValidationIssueKind::UndefinedConnection { name: name.clone() },
                        value.span,
                    ));
                }
            }
            "plugin" if !self.symbols.plugins.contains_key(name) => {
                self.issues.add_error(ValidationIssue::error(
                    ValidationIssueKind::UndefinedPlugin { name: name.clone() },
                    value.span,
                ));
            }
            _ => {}
        }
    }

    fn check_circular_dependencies(&mut self, document: &PplDocument) {
        // Check define block for circular references
        if let Some(define) = &document.define_block {
            for attr in &define.attributes {
                let mut visited = HashSet::new();
                let mut path = Vec::new();

                if let Some(cycle) = Self::find_define_cycle(
                    &attr.value,
                    &define.attributes,
                    &mut visited,
                    &mut path,
                ) {
                    self.issues.add_error(ValidationIssue::error(
                        ValidationIssueKind::CircularDefineDependency { chain: cycle },
                        attr.span,
                    ));
                }
            }
        }

        // Check pipelines dependencies
        let pipeline_deps = self.build_pipeline_dependency_graph(document);
        for pipeline in &document.pipelines {
            let mut visited = HashSet::new();
            let mut path = Vec::new();

            if let Some(cycle) =
                Self::find_pipeline_cycle(&pipeline.name, &pipeline_deps, &mut visited, &mut path)
            {
                self.issues.add_error(ValidationIssue::error(
                    ValidationIssueKind::CircularPipelineDependency { chain: cycle },
                    pipeline.span,
                ));
            }
        }
    }

    fn find_define_cycle(
        expr: &Expression,
        all_attrs: &[Attribute],
        visited: &mut HashSet<String>,
        path: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        // Extract referenced define constants from expression
        let refs = Self::extract_define_references(expr);

        for ref_name in refs {
            if let Some(cycle_start) = path.iter().position(|n| n == &ref_name) {
                // Found a cycle
                let mut cycle = path[cycle_start..].to_vec();
                cycle.push(ref_name);
                return Some(cycle);
            }

            if visited.contains(&ref_name) {
                continue;
            }

            visited.insert(ref_name.clone());
            path.push(ref_name.clone());

            // Find the attribute with this name and recurse
            if let Some(attr) = all_attrs.iter().find(|a| a.key.name == ref_name)
                && let Some(cycle) = Self::find_define_cycle(&attr.value, all_attrs, visited, path)
            {
                return Some(cycle);
            }

            path.pop();
        }

        None
    }

    fn extract_define_references(expr: &Expression) -> Vec<String> {
        let mut refs = Vec::new();

        match &expr.kind {
            ExpressionKind::DotNotation(path) => {
                if path.segments.len() == 2 && path.segments[0] == "define" {
                    refs.push(path.segments[1].clone());
                }
            }
            ExpressionKind::Binary { left, right, .. } => {
                refs.extend(Self::extract_define_references(left));
                refs.extend(Self::extract_define_references(right));
            }
            ExpressionKind::Unary { operand, .. } => {
                refs.extend(Self::extract_define_references(operand));
            }
            ExpressionKind::FunctionCall { arguments, .. } => {
                for arg in arguments {
                    refs.extend(Self::extract_define_references(arg));
                }
            }
            ExpressionKind::Array(elements) => {
                for elem in elements {
                    refs.extend(Self::extract_define_references(elem));
                }
            }
            ExpressionKind::WhenExpression {
                branches,
                else_value,
            } => {
                for branch in branches {
                    refs.extend(Self::extract_define_references(&branch.condition));
                    refs.extend(Self::extract_define_references(&branch.value));
                }
                if let Some(else_expr) = else_value {
                    refs.extend(Self::extract_define_references(else_expr));
                }
            }
            _ => {}
        }

        refs
    }

    fn build_pipeline_dependency_graph(
        &self,
        document: &PplDocument,
    ) -> HashMap<String, Vec<String>> {
        let mut graph = HashMap::new();

        for pipeline in &document.pipelines {
            let mut deps = Vec::new();

            if let Some(after_deps) = &pipeline.after {
                for expr in after_deps {
                    if let ExpressionKind::DotNotation(path) = &expr.kind
                        && path.segments.len() == 2
                        && path.segments[0] == "pipeline"
                    {
                        let dep_name = &path.segments[1];
                        deps.push(dep_name.clone());
                    }
                }
            }

            graph.insert(pipeline.name.clone(), deps);
        }

        graph
    }

    fn find_pipeline_cycle(
        current: &str,
        graph: &HashMap<String, Vec<String>>,
        visited: &mut HashSet<String>,
        path: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        if let Some(deps) = graph.get(current) {
            for dep in deps {
                if let Some(cycle_start) = path.iter().position(|n| n == dep) {
                    // Found a cycle
                    let mut cycle = path[cycle_start..].to_vec();
                    cycle.push(dep.clone());
                    return Some(cycle);
                }

                if visited.contains(dep) {
                    continue;
                }

                visited.insert(dep.clone());
                path.push(dep.clone());

                if let Some(cycle) = Self::find_pipeline_cycle(dep, graph, visited, path) {
                    return Some(cycle);
                }

                path.pop();
            }
        }
        None
    }

    fn check_unused_declarations(&mut self) {
        // Unused connections
        let unused_conns = self.symbols.get_unused_connections();
        for conn_name in unused_conns {
            if let Some(span) = self.symbols.connections.get(&conn_name) {
                self.issues.add_warning(ValidationIssue::warning(
                    ValidationIssueKind::UnusedConnection {
                        name: conn_name.clone(),
                    },
                    *span,
                ));
            }
        }

        // Unused define constants
        let unused_defs = self.symbols.get_unused_define_constants();
        for def_name in unused_defs {
            if let Some(span) = self.symbols.define_constants.get(&def_name) {
                self.issues.add_warning(ValidationIssue::warning(
                    ValidationIssueKind::UnusedDefineConstant {
                        name: def_name.clone(),
                    },
                    *span,
                ));
            }
        }
    }
}

impl Default for SemanticValidator {
    fn default() -> Self {
        Self::new()
    }
}

pub fn validate(document: &PplDocument) -> ValidationResult {
    SemanticValidator::new().validate(document)
}
