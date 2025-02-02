import geni.rspec.pg as pg

def catToFile(path: str, content: str) -> pg.Execute:
    if not content.endswith("\n"):
        content += "\n"
    return pg.Execute(
        shell="/bin/bash",
        command=f"sudo cat > {path} <<-EOF\n{content}EOF"
    )

def chmod(path: str, permissions: int, recursive: bool = False) -> pg.Execute:
    return pg.Execute(
        shell="/bin/bash",
        command=f"sudo chmod {'-R ' if recursive else ''}0{permissions:o} {path}"
    )

def ifaceForIp(ip: str) -> str:
    return f"sudo ifconfig | grep -B1 {ip} | grep -o '^\\w*'"
