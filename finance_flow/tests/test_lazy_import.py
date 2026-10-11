import subprocess
import sys


def _run(code: str) -> None:
    subprocess.run([sys.executable, "-c", code], check=True)


def test_locating_config_package_does_not_import_ccflow():
    # lerna resolves pkg://finance_flow.config on every Hydra app startup
    _run("import importlib.resources, sys; importlib.resources.files('finance_flow.config'); assert 'ccflow' not in sys.modules, sorted(sys.modules)")


def test_resolving_one_export_imports_only_its_submodule():
    _run(
        "import sys, finance_flow; finance_flow.MassiveDailyBarsFlatFileModel; assert 'finance_flow.massive_flat_files' in sys.modules; assert 'finance_flow.tasks' not in sys.modules"
    )


def test_star_import_and_dir_list_every_export():
    _run(
        "import finance_flow; names = set(finance_flow.__all__); assert {'MassiveDailyBarsFlatFileModel', 'BuildUniverseModel'} <= names; assert names <= set(dir(finance_flow))"
    )
