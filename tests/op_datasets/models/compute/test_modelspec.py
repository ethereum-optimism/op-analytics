import pytest
from op_analytics.datapipeline.models.compute.modelspec import ModelNode, ModelGraph


def test_model_graph_three_layers():
    """Test that ModelGraph correctly organizes nodes into three execution passes."""
    nodes = [
        ModelNode(name="A", inputs=["dataset_0"], outputs=["dataset_1"]),
        ModelNode(name="B", inputs=["dataset_0"], outputs=["dataset_2"]),
        ModelNode(name="C", inputs=["dataset_1"], outputs=["dataset_3"]),
        ModelNode(name="D", inputs=["dataset_2"], outputs=["dataset_4"]),
        ModelNode(name="E", inputs=["dataset_3", "dataset_4"], outputs=["dataset_5"]),
    ]

    graph = ModelGraph.of(nodes)

    # Check the number of passes
    assert len(graph.passes) == 3

    # First pass should contain independent nodes A and B
    assert set(graph.passes[0]) == {"A", "B"}

    # Second pass should contain nodes C and D that depend on first pass
    assert set(graph.passes[1]) == {"C", "D"}

    # Third pass should contain node E that depends on second pass
    assert set(graph.passes[2]) == {"E"}

    # Check if topological sort is correct
    # Note: The exact order within each pass might vary but should respect dependencies
    assert len(graph.sorted_nodes) == 5
    # Verify E comes after C and D
    e_index = graph.sorted_nodes.index("E")
    c_index = graph.sorted_nodes.index("C")
    d_index = graph.sorted_nodes.index("D")
    assert e_index > c_index and e_index > d_index


def test_model_graph_cycle_detection():
    """Test that ModelGraph correctly detects dependency cycles."""
    nodes = [
        ModelNode(name="A", inputs=["dataset_3"], outputs=["dataset_1"]),
        ModelNode(name="B", inputs=["dataset_1"], outputs=["dataset_2"]),
        ModelNode(name="C", inputs=["dataset_2"], outputs=["dataset_3"]),
    ]

    with pytest.raises(ValueError, match="Cycle detected in model dependencies"):
        ModelGraph.of(nodes)


def test_model_graph_self_cycle():
    """Test that ModelGraph detects self-referential cycles."""
    nodes = [
        ModelNode(name="A", inputs=["dataset_1"], outputs=["dataset_1"]),
    ]

    with pytest.raises(ValueError, match="Cycle detected in model dependencies"):
        ModelGraph.of(nodes)


def test_model_graph_empty():
    """Test that ModelGraph handles empty node list."""
    nodes: list[ModelNode] = []
    graph = ModelGraph.of(nodes)

    assert len(graph.passes) == 0
    assert len(graph.sorted_nodes) == 0
    assert len(graph.nodes) == 0


def test_model_graph_single_node():
    """Test that ModelGraph correctly handles a single node."""
    nodes = [
        ModelNode(name="A", inputs=[], outputs=["dataset_1"]),
    ]

    graph = ModelGraph.of(nodes)

    assert len(graph.passes) == 1
    assert graph.passes[0] == ["A"]
    assert graph.sorted_nodes == ["A"]
    assert len(graph.nodes) == 1
