#!/usr/bin/env python3
"""Exercise real engine build wiring in a disposable source checkout (requires Git/JDK)."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import zipfile
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--evidence-dir', type=Path, required=True)
args = parser.parse_args()
evidence = args.evidence_dir.resolve()
evidence.mkdir(parents=True, exist_ok=True)
repository = Path(__file__).resolve().parents[2]
environment = {key: value for key, value in os.environ.items()
               if not key.startswith('SPT_BUILD_') and key != 'SOURCE_DATE_EPOCH'}

with tempfile.TemporaryDirectory(prefix='spt-build-incrementality-') as temporary:
    checkout = Path(temporary)
    paths = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=repository).decode().split('\0')
    for name in paths:
        if not name or not (name.startswith(('engine/', 'test-fixtures/')) or name in ('VERSION', '.gitignore')):
            continue
        source = repository / name
        if not source.is_file():
            continue
        target = checkout / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    def git(*arguments):
        return subprocess.check_output(['git', '-c', 'user.name=Build Test', '-c', 'user.email=build-test@example.invalid', *arguments], cwd=checkout, stderr=subprocess.STDOUT)
    git('init', '-q')
    git('add', '.')
    git('commit', '-qm', 'Establish build fixture')
    engine = checkout / 'engine'
    resource = engine / 'core/spt-base/build/generated/resources/engineBuildInfo/META-INF/spt-build-info.properties'
    generate = ':core:spt-base:generateEngineBuildInfo'
    tasks = [generate, ':core:spt-base:jar', ':core:spt-base:shadowJar', ':core:spt-base:test', '--tests', 'com.dell.spt.base.buildinfo.*']
    def run(label, commands, success=True):
        log = evidence / (label + '.log')
        with log.open('w') as output:
            result = subprocess.run(['./gradlew', *commands, '--no-daemon', '--no-parallel', '--max-workers=2', '--build-cache', '--console=plain'], cwd=engine, env=environment, stdout=output, stderr=subprocess.STDOUT, text=True, timeout=600)
        text = log.read_text()
        assert (result.returncode == 0) == success, f'{label}: unexpected exit {result.returncode}; see log'
        if success and ':core:spt-base:test' in commands:
            reports = list((engine / 'core/spt-base/build/test-results/test').glob('TEST-*.xml'))
            assert reports, f'{label}: missing JUnit results'
            for report in reports:
                suite = ET.parse(report).getroot()
                assert int(suite.get('failures', 0)) + int(suite.get('errors', 0)) == 0, str(report)
        return text
    def values():
        return dict(line.split('=', 1) for line in resource.read_text().splitlines() if '=' in line)
    def assert_noop(output):
        for task in [generate, ':core:spt-base:processResources', ':core:spt-base:jar', ':core:spt-base:shadowJar', ':core:spt-base:test']:
            assert f'> Task {task} UP-TO-DATE' in output, f'{task} was not up to date'
    run('initial', tasks)
    initial = values()
    assert_noop(run('unchanged', tasks))
    assert values() == initial
    source = engine / 'core/spt-base/src/main/java/com/dell/spt/base/buildinfo/EngineBuildInfo.java'
    for number in (1, 2):
        previous = values()
        with source.open('a') as output:
            output.write(f'\n// Incrementality fixture edit {number}\n')
        run(f'source-change-{number}', [generate])
        assert values()['build_time'] != previous['build_time']
        assert values()['source_dirty'] == 'true'
    explicit = '-PsptBuildTime=2026-01-01T00:00:00Z'
    run('explicit-time', [*tasks, explicit])
    assert values()['build_time'] == '2026-01-01T00:00:00Z'
    assert_noop(run('explicit-unchanged', [*tasks, explicit]))
    changed = '-PsptBuildTime=2026-01-02T00:00:00Z'
    run('explicit-change', [*tasks, changed])
    expected = values()
    assert expected['build_time'] == '2026-01-02T00:00:00Z'
    resource.unlink()
    assert f'> Task {generate} FROM-CACHE' in run('restore-cache', [generate, changed])
    assert values() == expected
    checked_jars = 0
    for jar in (engine / 'core/spt-base/build/libs').glob('*.jar'):
        with zipfile.ZipFile(jar) as archive:
            if 'META-INF/spt-build-info.properties' not in archive.namelist():
                continue
            checked_jars += 1
            packaged = dict(line.split('=', 1) for line in archive.read('META-INF/spt-build-info.properties').decode().splitlines() if '=' in line)
            manifest = archive.read('META-INF/MANIFEST.MF').decode().replace('\r\n ', '')
            assert packaged == expected, jar
            assert 'Spt-Build-Time: ' + expected['build_time'] in manifest, jar
    assert checked_jars == 2, 'Expected the regular and shadow JARs'
    git('add', '.')
    git('commit', '-qm', 'Record fixture edits')
    release = [generate, '-PsptBuildRelease=true', explicit]
    run('release-clean', release)
    assert values()['source_dirty'] == 'false' and values()['development'] == 'false'
    original = source.read_bytes()
    source.write_bytes(original + b'\n// Dirty release fixture\n')
    assert 'Release engine source is dirty' in run('release-tracked-dirty', release, False)
    source.write_bytes(original)
    untracked = engine / 'untracked-release-input.txt'
    untracked.write_text('Release must reject untracked source\n')
    assert 'Release engine source is dirty' in run('release-untracked-dirty', release, False)
    print('PASS: unchanged builds, successive source edits, explicit times, cache restoration, manifest/resource agreement, and clean/dirty release checks')
