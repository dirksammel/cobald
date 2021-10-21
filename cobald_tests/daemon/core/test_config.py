from tempfile import NamedTemporaryFile

import pytest
import copy

from cobald.daemon.config.mapping import ConfigurationError
from cobald.daemon.core.config import load, COBalDLoader, yaml_constructor
from cobald.controller.linear import LinearController

from ...mock.pool import MockPool


# register test pool as safe for YAML configurations
COBalDLoader.add_constructor(tag="!MockPool", constructor=yaml_constructor(MockPool))


# Helpers for testing lazy/eager YAML evaluation
# Since YAML defaults to lazy evaluation, the arguments available during evaluation
# are not necessarily complete.
class TagTracker:
    """Helper to track the arguments supplied to YAML !Tags"""

    def __init__(self, *args, **kwargs):
        # the state of arguments *during* YAML evaluation
        self.orig_args = copy.deepcopy(args)
        self.orig_kwargs = copy.deepcopy(kwargs)
        # the state of arguments *after* YAML evaluation
        self.final_args = args
        self.final_kwargs = kwargs


COBalDLoader.add_constructor(
    tag="!TagTracker", constructor=yaml_constructor(TagTracker, eager=True)
)
COBalDLoader.add_constructor(
    tag="!TagTrackerLazy", constructor=yaml_constructor(TagTracker, eager=False)
)


# Helpers to test how ``yaml_constructor`` behaves
# with ("full") and without ("bare") arguments
@yaml_constructor(eager=True)
def full_deco_constructor(*args, **kwargs):
    """YAML tag constructor that is explicitly eager"""
    return copy.deepcopy(args), copy.deepcopy(kwargs)


@yaml_constructor
def bare_deco_constructor(*args, **kwargs):
    """YAML tag constructor that is implicitly lazy"""
    return copy.deepcopy(args), copy.deepcopy(kwargs)


COBalDLoader.add_constructor(
    tag="!TagConstructorFull", constructor=full_deco_constructor
)
COBalDLoader.add_constructor(
    tag="!TagConstructorBare", constructor=bare_deco_constructor
)


def get_config_section(config: dict, section: str):
    return next(
        content for plugin, content in config.items() if plugin.section == section
    )


class TestYamlConfig:
    def test_load(self):
        """Load a valid YAML config"""
        with NamedTemporaryFile(suffix=".yaml") as config:
            with open(config.name, "w") as write_stream:
                write_stream.write(
                    """
                    pipeline:
                        - !LinearController
                          low_utilisation: 0.9
                          high_allocation: 1.1
                        - !MockPool
                    """
                )
            with load(config.name):
                assert True
            assert True

    def test_load_invalid(self):
        """Load a invalid YAML config (invalid keyword argument)"""
        with NamedTemporaryFile(suffix=".yaml") as config:
            with open(config.name, "w") as write_stream:
                write_stream.write(
                    """
                    pipeline:
                        - !LinearController
                          low_utilisation: 0.9
                          foo: 0
                        - !MockPool
                    """
                )
            with pytest.raises(TypeError):
                with load(config.name):
                    assert False

    def test_load_dangling(self):
        """Forbid loading a YAML config with dangling content"""
        with NamedTemporaryFile(suffix=".yaml") as config:
            with open(config.name, "w") as write_stream:
                write_stream.write(
                    """
                    pipeline:
                        - !LinearController
                          low_utilisation: 0.9
                          high_allocation: 1.1
                        - !MockPool
                    random_things:
                        foo: bar
                    """
                )
            with pytest.raises(ConfigurationError):
                with load(config.name):
                    assert False

    def test_load_missing(self):
        """Forbid loading a YAML config with missing content"""
        with NamedTemporaryFile(suffix=".yaml") as config:
            with open(config.name, "w") as write_stream:
                write_stream.write(
                    """
                    logging:
                        version: 1.0
                    """
                )
            with pytest.raises(ConfigurationError):
                with load(config.name):
                    assert False

    def test_load_mixed_creation(self):
        """Load a YAML config with mixed pipeline step creation methods"""
        with NamedTemporaryFile(suffix=".yaml") as config:
            with open(config.name, "w") as write_stream:
                write_stream.write(
                    """
                    pipeline:
                        - __type__: cobald.controller.linear.LinearController
                          low_utilisation: 0.9
                          high_allocation: 0.9
                        - !MockPool
                    """
                )
            with load(config.name) as config:
                pipeline = get_config_section(config, "pipeline")
                assert isinstance(pipeline[0], LinearController)
                assert isinstance(pipeline[0].target, MockPool)

    def test_load_tags_substructure(self):
        """Load !Tags with substructure"""
        with NamedTemporaryFile(suffix=".yaml") as config:
            with open(config.name, "w") as write_stream:
                write_stream.write(
                    """
                    pipeline:
                        - !MockPool
                    __config_test__:
                        tagged: !TagTracker
                          host: 127.0.0.1
                          port: 1234
                          algorithm: HS256
                          users:
                            - user_name: tardis
                              scopes:
                                - user:read
                    """
                )
            with load(config.name) as config:
                tagged = get_config_section(config, "__config_test__")["tagged"]
                assert isinstance(tagged, TagTracker)
                assert tagged.final_kwargs["host"] == "127.0.0.1"
                assert tagged.final_kwargs["port"] == 1234
                assert tagged.final_kwargs["algorithm"] == "HS256"
                assert tagged.final_kwargs["users"][0]["user_name"] == "tardis"
                assert tagged.final_kwargs["users"][0]["scopes"] == ["user:read"]

    def test_load_tags_nested(self):
        """Load !Tags with nested !Tags"""
        with NamedTemporaryFile(suffix=".yaml") as config:
            with open(config.name, "w") as write_stream:
                write_stream.write(
                    """
                    pipeline:
                        - !MockPool
                    __config_test__:
                        tagged: !TagTracker
                          host: 127.0.0.1
                          port: 1234
                          algorithm: HS256
                          users: !TagTracker
                            - user_name: tardis
                              scopes:
                                - user:read
                    """
                )
            with load(config.name) as config:
                top_tag = get_config_section(config, "__config_test__")["tagged"]
                assert top_tag.final_kwargs["host"] == "127.0.0.1"
                assert top_tag.final_kwargs["port"] == 1234
                assert top_tag.final_kwargs["algorithm"] == "HS256"
                sub_tag = top_tag.final_kwargs["users"]
                assert isinstance(sub_tag, TagTracker)
                assert sub_tag.final_args[0]["scopes"] == ["user:read"]

    def test_load_tags_eager(self):
        """Load !Tags with substructure, immediately using them"""
        with NamedTemporaryFile(suffix=".yaml") as config:
            with open(config.name, "w") as write_stream:
                write_stream.write(
                    """
                    pipeline:
                        - !MockPool
                    __config_test__:
                        tagged: !TagTracker
                          top: "top level value"
                          nested:
                            - leaf: "leaf level value"
                    """
                )
            with load(config.name) as config:
                tagged = get_config_section(config, "__config_test__")["tagged"]
                assert isinstance(tagged, TagTracker)
                assert tagged.orig_kwargs["top"] == "top level value"
                assert isinstance(tagged.orig_kwargs["nested"], list)
                assert len(tagged.orig_kwargs["nested"]) > 0
                assert tagged.orig_kwargs["nested"] == [{"leaf": "leaf level value"}]

    def test_load_tags_lazy(self):
        """Load !Tags with substructure, lazily using them"""
        with NamedTemporaryFile(suffix=".yaml") as config:
            with open(config.name, "w") as write_stream:
                write_stream.write(
                    """
                    pipeline:
                        - !MockPool
                    __config_test__:
                        tagged: !TagTrackerLazy
                          top: "top level value"
                          nested:
                            - leaf: "leaf level value"
                    """
                )
            with load(config.name) as config:
                tagged = get_config_section(config, "__config_test__")["tagged"]
                assert isinstance(tagged, TagTracker)
                assert tagged.orig_kwargs["top"] == "top level value"
                assert isinstance(tagged.orig_kwargs["nested"], list)
                assert len(tagged.orig_kwargs["nested"]) == 0
                assert len(tagged.final_kwargs["nested"]) > 0
                assert tagged.orig_kwargs["nested"] == []
                assert tagged.final_kwargs["nested"] == [{"leaf": "leaf level value"}]

    def test_load_tags_decotype(self):
        """Load !Tags with decorator coverage"""
        with NamedTemporaryFile(suffix=".yaml") as config:
            with open(config.name, "w") as write_stream:
                write_stream.write(
                    """
                    pipeline:
                        - !MockPool
                    __config_test__:
                        tagged_full: !TagConstructorFull
                          top: "top level value"
                          nested:
                            - leaf: "leaf level value"
                        tagged_bare: !TagConstructorBare
                          top: "top level value"
                          nested:
                            - leaf: "leaf level value"
                    """
                )
            with load(config.name) as config:
                section = get_config_section(config, "__config_test__")
                tagged_full = section["tagged_full"]
                tagged_bare = section["tagged_bare"]
                assert isinstance(tagged_full, tuple)
                assert isinstance(tagged_bare, tuple)
                assert tagged_full[1]["top"] == "top level value"
                assert tagged_bare[1]["top"] == "top level value"
                assert tagged_bare[1]["nested"] == []
                assert tagged_full[1]["nested"] == [{"leaf": "leaf level value"}]
