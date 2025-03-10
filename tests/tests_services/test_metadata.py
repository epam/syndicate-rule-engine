from services.metadata import merge_metadata, Metadata, EMPTY_DOMAIN_METADATA


def test_merge_metadata(load_metadata):
    one = load_metadata('metrics_metadata', Metadata)
    two = load_metadata('test_collection_data_source', Metadata)
    three = Metadata(
        domains={'AZURE': EMPTY_DOMAIN_METADATA}
    )
    result = merge_metadata(one, two, three)
    assert len(result.rules) == 12
    assert len(result.domains) == 3