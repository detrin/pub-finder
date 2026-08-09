from pathlib import Path


def test_ci_runs_the_repositorys_javascript_test_suite():
    workflow = Path(".github/workflows/ci.yml").read_text()

    assert "node --test tests/js/*.test.js" in workflow
    assert "tests_js/*.test.mjs" not in workflow
