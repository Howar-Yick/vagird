# 仅做“能否编译通过”的冒烟测试，不导入 PTRADE 运行环境
import pathlib


def test_vagird_exists_and_compiles():
    p = pathlib.Path(r"D:\OneDrive\vagird\vagird.py")
    assert p.exists(), "vagird.py 不存在，请确认路径"
    code = p.read_text(encoding="utf-8")
    # 编译（不执行），能过说明无明显语法错误
    compile(code, str(p), "exec")
