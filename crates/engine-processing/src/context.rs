use crate::io::{destination::Destination, source::Source};
use engine_core::context::exec::ExecutionContext;
use engine_state::StateStore;
use engine_wasm::registry::PluginRegistry;
use model::{
    execution::pipeline::Pipeline, pagination::cursor::Cursor,
    transform::mapping::TransformationMetadata,
};
use query_builder::offsets::OffsetStrategy;
use std::sync::Arc;

/// Represents the execution context for a single pipeline in the migration process.
pub struct PipelineContext {
    pub exec_ctx: Arc<ExecutionContext>,
    pub run_id: String,
    pub item_id: String,
    pub source: Source,
    pub destination: Destination,
    pub pipeline: Pipeline,
    pub mapping: TransformationMetadata,
    pub state: Arc<StateStore>,
    pub offset_strategy: Arc<dyn OffsetStrategy>,
    pub cursor: Cursor,
    pub plugin_registry: Arc<PluginRegistry>,
}

impl PipelineContext {
    pub fn builder(exec_ctx: Arc<ExecutionContext>) -> PipelineContextBuilder {
        PipelineContextBuilder::new(exec_ctx)
    }
}

pub struct PipelineContextBuilder {
    exec_ctx: Arc<ExecutionContext>,
    run_id: Option<String>,
    item_id: Option<String>,
    source: Option<Source>,
    destination: Option<Destination>,
    pipeline: Option<Pipeline>,
    mapping: Option<TransformationMetadata>,
    state: Option<Arc<StateStore>>,
    offset_strategy: Option<Arc<dyn OffsetStrategy>>,
    cursor: Option<Cursor>,
    plugin_registry: Option<Arc<PluginRegistry>>,
}

impl PipelineContextBuilder {
    fn new(exec_ctx: Arc<ExecutionContext>) -> Self {
        Self {
            exec_ctx,
            run_id: None,
            item_id: None,
            source: None,
            destination: None,
            pipeline: None,
            mapping: None,
            state: None,
            offset_strategy: None,
            cursor: None,
            plugin_registry: None,
        }
    }

    pub fn run_id(mut self, run_id: String) -> Self {
        self.run_id = Some(run_id);
        self
    }

    pub fn item_id(mut self, item_id: String) -> Self {
        self.item_id = Some(item_id);
        self
    }

    pub fn source(mut self, source: Source) -> Self {
        self.source = Some(source);
        self
    }

    pub fn destination(mut self, destination: Destination) -> Self {
        self.destination = Some(destination);
        self
    }

    pub fn pipeline(mut self, pipeline: Pipeline) -> Self {
        self.pipeline = Some(pipeline);
        self
    }

    pub fn mapping(mut self, mapping: TransformationMetadata) -> Self {
        self.mapping = Some(mapping);
        self
    }

    pub fn state(mut self, state: Arc<StateStore>) -> Self {
        self.state = Some(state);
        self
    }

    pub fn offset_strategy(mut self, offset_strategy: Arc<dyn OffsetStrategy>) -> Self {
        self.offset_strategy = Some(offset_strategy);
        self
    }

    pub fn cursor(mut self, cursor: Cursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    pub fn plugin_registry(mut self, plugin_registry: Arc<PluginRegistry>) -> Self {
        self.plugin_registry = Some(plugin_registry);
        self
    }

    pub fn build(self) -> PipelineContext {
        PipelineContext {
            exec_ctx: self.exec_ctx,
            run_id: self.run_id.expect("run_id is required"),
            item_id: self.item_id.expect("item_id is required"),
            source: self.source.expect("source is required"),
            destination: self.destination.expect("destination is required"),
            pipeline: self.pipeline.expect("pipeline is required"),
            mapping: self.mapping.expect("mapping is required"),
            state: self.state.expect("state is required"),
            offset_strategy: self.offset_strategy.expect("offset_strategy is required"),
            cursor: self.cursor.unwrap_or(Cursor::None),
            plugin_registry: self.plugin_registry.expect("plugin_registry is required"),
        }
    }
}
