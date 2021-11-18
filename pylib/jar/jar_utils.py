
import zipfile


def get_jar_classes(jar_path):
    jar = zipfile.ZipFile(jar_path, 'r')
    jar_classes = [f.replace("/", ".").replace('.class', '') for f in jar.namelist() if '.class' in f]
    return jar_classes


def get_jar_manifest_attributes(jar_path):
    jar = zipfile.ZipFile(jar_path, 'r')
    manifest_file = jar.read(name="META-INF/MANIFEST.MF")
    rows = manifest_file.splitlines()
    for idx, row in enumerate(rows):
        if row.startswith(" "):
            rows[idx-1] = rows[idx] + row[1:]
            rows[idx] = ""
    # will fail for long attributes (MANIFEST.MF breaks them to multiple rows)
    return {row.split(": ")[0]: row.split(": ")[1] for row in rows if ": " in row}
