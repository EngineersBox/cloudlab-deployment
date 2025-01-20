import geni.rspec.pg as pg

def catToFile(path: str, content: str) -> pg.Execute:
    if not content.endswith("\n"):
        content += "\n"
    return pg.Execute(
        shell="/bin/bash",
        command=f"sudo cat > {path} <<-EOF\n{content}EOF"
    )
