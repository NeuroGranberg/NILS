from extract.resume_index import ExistingPathIndex, SubjectPathEntry, split_subject_relative


def test_split_subject_relative_handles_edge_cases():
    assert split_subject_relative("subject/file.dcm") == ("subject", "file.dcm")
    assert split_subject_relative("subject") == ("subject", "")
    assert split_subject_relative("/subject//nested/file.dcm") == ("subject", "nested/file.dcm")


def test_existing_path_index_switches_to_bloom():
    entry = SubjectPathEntry(threshold=1, error_rate=0.001)
    entry.add("first.dcm")
    # Threshold of 1 forces bloom
    entry.add("second.dcm")
    assert entry.contains("first.dcm")
    assert entry.contains("second.dcm")


def test_existing_path_index_lookup():
    index = ExistingPathIndex()
    index.add("subjectX", "a/b/c.dcm")
    assert index.should_skip("subjectX", "a/b/c.dcm")
    assert not index.should_skip("subjectX", "new/file.dcm")
    assert not index.should_skip("other", "a/b/c.dcm")
