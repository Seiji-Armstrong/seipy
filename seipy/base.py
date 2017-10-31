import subprocess


def issue_shell_command(cmd: str, my_env=None):
    """
    Issues a command in a shell and returns the result as str.

    Parameters:
    cmd - command to be issued (str)

    In python3.x, stdout,stderr are both b'' (byte string literal: bytes object)
        and must be decoded to UTF-8 for string concatenation etc
    Example usage (simple):
    >> issue_shell_command(cmd="ls")
    Example usage (more involved):
    >> s3dir = "s3://..."; issue_shell_command("aws s3 ls --recursive {}".format(s3dir))
    """
    pipe = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, env=my_env)

    return pipe.stdout.strip().decode('UTF-8') + '\n' + pipe.stderr.strip().decode('UTF-8')