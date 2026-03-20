from sources.registry import get_source_connector, list_source_connectors



def test_multi_source_registry_exposes_expected_connectors() -> None:
    connectors = {connector.slug: connector for connector in list_source_connectors()}

    assert sorted(connectors) == ["biddit", "immovlan", "immoweb", "notaire"]
    assert connectors["biddit"].source_name == "Biddit"
    assert connectors["immovlan"].default_output_path.name == "immovlan_latest.jsonl"
    assert connectors["notaire"].default_output_path.name == "notaire_latest.jsonl"
    assert get_source_connector("immoweb").source_name == "Immoweb"

