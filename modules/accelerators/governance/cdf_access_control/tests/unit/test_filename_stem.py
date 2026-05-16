from governance_build.context import filename_stem_from_name


def test_filename_stem_from_instance_space():
    assert filename_stem_from_name("inst_site_a") == "inst_site_a"


def test_filename_stem_sanitizes_whitespace():
    assert filename_stem_from_name("My Space Name") == "my_space_name"
