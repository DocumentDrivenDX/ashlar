"""Named operator-trusted installed runtime profiles and full original custody.

Content correspondence does not authenticate an arbitrary tool. This supported
local profile assumes genuine operator-admitted Homebrew deployment and Darwin
loader/system frameworks/host isolation. Opening/closing proves correspondence,
not exclusion of transient mutation. No credential caches are read here.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import hashlib
import os
import platform
import pwd
import sys
import stat
import time
from .lifecycle import finish

CLI_ROOT=Path('/opt/homebrew/Cellar/azure-cli/2.90.0')
PYTHON_ROOT=Path('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14')
CLI_PYTHON=CLI_ROOT/'libexec/bin/python3.14'
HTTP_PYTHON=PYTHON_ROOT/'bin/python3.14'
CA_BUNDLE=CLI_ROOT/'libexec/lib/python3.14/site-packages/certifi/cacert.pem'
NATIVE_DEPENDENCIES=(('/opt/homebrew/opt/libsodium/lib/libsodium.26.dylib', '/opt/homebrew/Cellar/libsodium/1.0.22/lib/libsodium.26.dylib'), ('/opt/homebrew/opt/libyaml/lib/libyaml-0.2.dylib', '/opt/homebrew/Cellar/libyaml/0.2.5/lib/libyaml-0.2.dylib'), ('/opt/homebrew/opt/mpdecimal/lib/libmpdec.4.dylib', '/opt/homebrew/Cellar/mpdecimal/4.0.1/lib/libmpdec.4.0.1.dylib'), ('/opt/homebrew/opt/openssl@3/lib/libcrypto.3.dylib', '/opt/homebrew/Cellar/openssl@3/3.6.5/lib/libcrypto.3.dylib'), ('/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib', '/opt/homebrew/Cellar/openssl@3/3.6.5/lib/libssl.3.dylib'), ('/opt/homebrew/opt/openssl@4/lib/libcrypto.4.dylib', '/opt/homebrew/Cellar/openssl@4/4.0.3/lib/libcrypto.4.dylib'), ('/opt/homebrew/opt/openssl@4/lib/libssl.4.dylib', '/opt/homebrew/Cellar/openssl@4/4.0.3/lib/libssl.4.dylib'), ('/opt/homebrew/opt/python@3.14/Frameworks/Python.framework/Versions/3.14/Python', '/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/Python'), ('/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib', '/opt/homebrew/Cellar/sqlite/3.54.0/lib/libsqlite3.3.54.0.dylib'), ('/opt/homebrew/opt/xz/lib/liblzma.5.dylib', '/opt/homebrew/Cellar/xz/5.8.4/lib/liblzma.5.dylib'), ('/opt/homebrew/opt/zstd/lib/libzstd.1.dylib', '/opt/homebrew/Cellar/zstd/1.5.7_1/lib/libzstd.1.5.7.dylib'))
PROVENANCE_PINS=(('/opt/homebrew/Cellar/azure-cli/2.90.0/bin/az', '32b7f9d649e528addf58840362c24c5850da20e20bc32e01efc6afc7f73d50b0'), ('/opt/homebrew/Cellar/azure-cli/2.90.0/libexec/pyvenv.cfg', '72fd5d34bcf5a7624cc79f6c01fb5b8d5b32f6bedf9c6c9cf6475b00fc34e90f'), ('/opt/homebrew/Cellar/azure-cli/2.90.0/INSTALL_RECEIPT.json', 'b1c11a3925fdc60f5702552d2191c219e32b651aa3f2e5e011787fc05e85c1c0'), ('/opt/homebrew/Cellar/azure-cli/2.90.0/libexec/lib/python3.14/site-packages/distutils-precedence.pth', '2638ce9e2500e572a5e0de7faed6661eb569d1b696fcba07b0dd223da5f5d224'), ('/opt/homebrew/Cellar/azure-cli/2.90.0/libexec/lib/python3.14/site-packages/certifi/cacert.pem', '488ba960602bf07cc63f4ef7aec108692fec41820fc3328a8e3f3de038149aee'))
NATIVE_PINS=(('/opt/homebrew/Cellar/azure-cli/2.90.0/libexec/lib/python3.14/site-packages/_cffi_backend.cpython-314-darwin.so', 'e248d579066ab6c44d06a5dfdb4d1516b9e69fc8efaeb9e301ab07373b06ca0e'), ('/opt/homebrew/Cellar/azure-cli/2.90.0/libexec/lib/python3.14/site-packages/bcrypt/_bcrypt.abi3.so', '5979b0f8f51cee6fd54b2a9e65e3121d42b2c9bbb821bca6b1728e383f407f48'), ('/opt/homebrew/Cellar/azure-cli/2.90.0/libexec/lib/python3.14/site-packages/cryptography/hazmat/bindings/_rust.abi3.so', '539209dc9ddecfa28c9b389c3b8623f2fee94c8d655297c867c638373ca0abd2'), ('/opt/homebrew/Cellar/azure-cli/2.90.0/libexec/lib/python3.14/site-packages/nacl/_sodium.abi3.so', '8059b0d75428a158f1c660e2d7531d8796d7d51f94b743d9eedca7ce30e171f0'), ('/opt/homebrew/Cellar/azure-cli/2.90.0/libexec/lib/python3.14/site-packages/psutil/_psutil_osx.abi3.so', '62d38db5b2a816372264de76885f9361e7e20e02197146b72837998f687185a0'), ('/opt/homebrew/Cellar/azure-cli/2.90.0/libexec/lib/python3.14/site-packages/psutil/_psutil_posix.abi3.so', 'a3a82ad98078a6b0a83836f802b21a557111e93f774cc1be144cc00d3e14fe3c'), ('/opt/homebrew/Cellar/azure-cli/2.90.0/libexec/lib/python3.14/site-packages/wrapt/_wrappers.cpython-314-darwin.so', '209ca1b3c52757b32ca54f060f61e92f691a02ed5b34c64b330cce6ff3e83146'), ('/opt/homebrew/Cellar/azure-cli/2.90.0/libexec/lib/python3.14/site-packages/yaml/_yaml.cpython-314-darwin.so', '0485f11807b0807788524f58f8bb0c51c59a1ac4558e5c8043810ac772f43ea3'), ('/opt/homebrew/Cellar/libsodium/1.0.22/lib/libsodium.26.dylib', '3e831034a31fc26d740917a07e9141db0699fc9cc4737a31e532c02145e0a212'), ('/opt/homebrew/Cellar/libyaml/0.2.5/lib/libyaml-0.2.dylib', 'decd43b62fbabea6bfc049380c6111a2838fddd7f8fd345b63b44fc8471dff8e'), ('/opt/homebrew/Cellar/mpdecimal/4.0.1/lib/libmpdec.4.0.1.dylib', 'e5cbe1e8bb051e40ad32c0184c42366cf813c80a731a8424de0dd7f16ac90fde'), ('/opt/homebrew/Cellar/openssl@3/3.6.5/lib/libcrypto.3.dylib', '6c7139ae515b274286b2f62d662a173e32d34854e1fc26cca46ee08efe16ae45'), ('/opt/homebrew/Cellar/openssl@3/3.6.5/lib/libssl.3.dylib', '2b343dc7a88d6e49720ae6b939a178b5e688912e682044250203770b8acc2a08'), ('/opt/homebrew/Cellar/openssl@4/4.0.3/lib/libcrypto.4.dylib', 'eb4e14b7f6ef10df40a60f7a078920c44e6b24be23f76eb1f657142becfeb4e7'), ('/opt/homebrew/Cellar/openssl@4/4.0.3/lib/libssl.4.dylib', 'd223211e5bc6bfa664a8e066024d026f95f5b571c7b17376312344710d142aa7'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/Python', 'ea96cfe3ee0190f17c4f907a321f8aadbca35eee47285e329ad243347be6a8b9'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/bin/python3.14', '1b5cb11d163e5ba34e2a05fc533c0ed89574d4425f9749a9d89405f651980e0d'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_asyncio.cpython-314-darwin.so', '61cdf07c8c4b2298fc7643b41b433bbd0997f1e5759e4bd689737700e1ebd0c0'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_bisect.cpython-314-darwin.so', '9f6c0bbe2feb88d6d4e6c84023af4ebaafe43271b41cbb512d3a0ecf162f4f60'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_blake2.cpython-314-darwin.so', '81fbac5b7bba8375a414db334f0890b61d7f7a64fdfc7300adc9276b7e02b3a9'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_bz2.cpython-314-darwin.so', 'f429a451efef8e0bd4de6dba140c4081e57eafad00411a38d5c3c33f38c0950a'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_codecs_cn.cpython-314-darwin.so', 'd547d2430d2d5ebdb719037c4e87c1c1fff09b1d296a784a232055c1b99b0552'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_codecs_hk.cpython-314-darwin.so', '8d48b24ac9cf0f8ce5c58861cb59794a065f8660dbbc297c02c4507f2525690b'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_codecs_iso2022.cpython-314-darwin.so', '79422f0127ead57b6aedd21855fbdd05a1cdffa8bc4c712e3515454d4e482509'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_codecs_jp.cpython-314-darwin.so', '3192917a5ada8c0abaed01651a4ac84799573ce9e41d89f678e2156ec0bda34a'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_codecs_kr.cpython-314-darwin.so', 'c9b112162d4b4b60e65102f5b406f8f69a6665feef7fb49584d6e8ef8b82c9bf'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_codecs_tw.cpython-314-darwin.so', '63f2b7cc1315f535d80afed698bd126195c64cde65afe20470e6043134471e76'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_csv.cpython-314-darwin.so', 'c97f21e4f644a32d6a71099c1196c3f40c239ef2abba08c5f2a5a38208d7ae88'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_ctypes.cpython-314-darwin.so', '44ef75b0b2403bdd7a2bf65ef0c516ffb01b405dd79b6407e1bf0e6f95a2af3a'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_ctypes_test.cpython-314-darwin.so', '4329c83ede846e4a1d6bfbb9693568ab0735747ccf1ad65c489134d5d6731b01'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_curses.cpython-314-darwin.so', '6d85f5cb2f9e638eb4bb435c67103bb9aee18e377d921a59ba9bfef479db897b'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_curses_panel.cpython-314-darwin.so', 'facb27abaaecc9b6668a0daaed3f82c64bf2cd271ff8c02b94ebb9d5b60309ba'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_dbm.cpython-314-darwin.so', '2559175a4b7ddbd94e2422bbfb167642a81c6fe50533ff11b946f7a668889b6f'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_decimal.cpython-314-darwin.so', 'f9bcce10f69ce4360d35af8145f8de627f66cb3b76a4c4ba16af2698c38b2cbd'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_elementtree.cpython-314-darwin.so', '09588733904abcc3fd68a1197e84e4bf5441e37a5edc4f30719b87133cd52ff0'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_hashlib.cpython-314-darwin.so', '0d4121b454edf532617370d8ef0332596cf60be4caea65f1f691f40212e3a119'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_heapq.cpython-314-darwin.so', '4c4d28d5e6cb87bf13d52a1ad994ec9f445ff3b59e11aad8cd1ec48f1e047b99'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_hmac.cpython-314-darwin.so', '6cf0480553d6b12688840df845dc33292729e644211368281068ef5c984db6de'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_interpchannels.cpython-314-darwin.so', '4b7b394eb490f4011e33d3395c8ffacfec9b078a6cb651a22caf437f98af308e'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_interpqueues.cpython-314-darwin.so', '9caf09c8dd246fc0fb0cb4553af978364046e6c9047f63e85957b4308a9ae163'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_interpreters.cpython-314-darwin.so', '1f27b6573edb1a7f6d762182e70d0c55cd578ff868e76c1cdf4e315f88c3b3d1'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_json.cpython-314-darwin.so', '0842d197f955864b038aae85fa7d264a00f1eb4a94234fc9868cad8776d798bf'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_lsprof.cpython-314-darwin.so', '9c6946a128889c722b122453838bb4ebf6288b2e5482d43d0e4a5ef01eefe68d'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_lzma.cpython-314-darwin.so', 'd9727e0a6b1a0831b6feda85db2c566b5a3427e981059a6ae7883505d0a2a273'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_md5.cpython-314-darwin.so', '22d92c7ef9a2bfd5b7c9fd0a0fe3ebb18e0427e900c5738da6556572ed8b18c8'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_multibytecodec.cpython-314-darwin.so', '66701cae4f5f545983047dda7c2a5d517dea0c1880cdbf5fc2a91f50f772a5a2'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_multiprocessing.cpython-314-darwin.so', '2b43ce689f1e7f4ad8688fec82d05448465c6e2220446b6067d46389871897d7'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_pickle.cpython-314-darwin.so', '70809498578e0b84b6dbe1b0e34463135201875bc5dc4989740334b6865d2a5f'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_posixshmem.cpython-314-darwin.so', 'bda5aa486cfeb60aa4966377bda5e527f331424e913ff2b4ff98b72c2492be36'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_posixsubprocess.cpython-314-darwin.so', 'a75d3577267410c583cf04989b571a38d99fe6512310f4006e22350876238e75'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_queue.cpython-314-darwin.so', '3a2d461636396e1ec0468dbe559e55169df028ac20776d815f70ebd7c9028126'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_random.cpython-314-darwin.so', 'a39b2ce7d28cc393b2ea43a70bae50ee040e19fb19edd302182ec6546f8079d4'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_remote_debugging.cpython-314-darwin.so', '6f92b8c5768f3f17156cdcccebb1fb7513880db70a5af63d5e2a84bdf4803db0'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_scproxy.cpython-314-darwin.so', '44d8416915126d519ec8ab52d145dcb17b2bbb5b7590365633dff80a98efd277'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_sha1.cpython-314-darwin.so', '6b2dedcb582969960262dcc8c259422351e2452998f6271231ed074fe92341cd'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_sha2.cpython-314-darwin.so', 'de2d41fb0711c6f18dc48e818942b04d0bd4d16862a883f3654f44c9fa1590c2'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_sha3.cpython-314-darwin.so', 'f13193fceac2742a3ec009f537b80e84cc90c822f8f7df590b51b96c6aa30447'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_socket.cpython-314-darwin.so', '6227de7ef657172d8704fbbc9f97847aa32b5ef61d0641dad93b42b608b22b25'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_sqlite3.cpython-314-darwin.so', 'b0b48a67da597047a3f8715be2ef807ba0aa1d3da13961024529e0f08145928c'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_ssl.cpython-314-darwin.so', 'd95b8a7bb309c7b93753360898e29f1435839faf8901e9a8f92c03f7284a46bf'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_statistics.cpython-314-darwin.so', 'db9364e9b0261901d2e609d9c01d30876b88eae0f30a8edc891dd67b489c0b25'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_struct.cpython-314-darwin.so', '9c17544a2a41fcb18de4ceb7137687d610adaf7a0e74a6c0f7443e23e2cab488'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_testbuffer.cpython-314-darwin.so', '4651e3211e34dc562d48564020be45376262d986807292993aafb268b996337f'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_testcapi.cpython-314-darwin.so', '32cd442e253f8342d2a0205e90cb213bcbc795f12a1a20ed41a6f1ffe32d2762'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_testclinic.cpython-314-darwin.so', '68a325b585dbb3d7a103ec199beea77207115e04252c727c32197aeabbb48bf4'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_testclinic_limited.cpython-314-darwin.so', 'fdfd42f4f73de02f987914fe708bfee75ecb8d9d08e5c50fc6ad88907567bafc'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_testimportmultiple.cpython-314-darwin.so', 'bee3f576c84bbbb9fbb84624772fc317f08615d28759d1f75cb6bd7093478b33'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_testinternalcapi.cpython-314-darwin.so', '508152d91fcf089faff9b3669ab7d93d3decfda47a45c1b1c8ca33edf1950f41'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_testlimitedcapi.cpython-314-darwin.so', 'b50e9fd8a2a6db36dbdc5296e3c05f733d83864cc455fcae9e5aa62682d39bea'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_testmultiphase.cpython-314-darwin.so', '55aeabcadbd05996018f7937d987ecd08f851ec8ca21cf03a27584322453fdaa'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_testsinglephase.cpython-314-darwin.so', '0bcbe57256946a7c6214a0b078eaf7fcf7a9c93b9ec9ae07919fa0027e22e27c'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_uuid.cpython-314-darwin.so', '4b3cc6b0b50945cf2362a295aa13ea720e6beb377ebba8b4f9f630420d3c4740'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_xxtestfuzz.cpython-314-darwin.so', '7346cf2f361bf18c3b0ad6f4ca3f696faeb8453888a34b229e631e7dbd407728'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_zoneinfo.cpython-314-darwin.so', '8314eea7280b85b0ade0af854c35c1cdc18ea8a3f64b81fb1c84565e72ef42fc'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/_zstd.cpython-314-darwin.so', '48fed250af04e31c8eb1e76a163258da1ae25d7b8a42865eb121d854d29631f4'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/array.cpython-314-darwin.so', 'c01a85581a716bca84971637ea56a14ea1cb610b3bb6f698b94e4392718ed951'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/binascii.cpython-314-darwin.so', '35cf670d41995767e096d54e99bad35225ec316bfa365aca8a2d5b7d79568d3d'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/cmath.cpython-314-darwin.so', 'cb4185ecccc1a4c9dfc1ab1787873cd7414d0cdb9dc2197ad45cd6efef473d33'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/fcntl.cpython-314-darwin.so', '7b25bc09518e2a9cb7ab5555de3fdf1876c4d63b573e3e2715be753fdca22b06'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/grp.cpython-314-darwin.so', '6622e9c8afbee811a6b8d43789dddf6a39d11fc3c2109f6296251470e51953b3'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/math.cpython-314-darwin.so', 'de5b80566a649f52c2c8bb03397a1f806797d86f40360fa4bec205ee25a5e048'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/mmap.cpython-314-darwin.so', '28cdb96fdb3d38a364360a397640ce3a1044337866811e99196be2fffb92cd76'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/pyexpat.cpython-314-darwin.so', '9b393f046b28e1b9de9bc2b4bb0fe54a57d44b6b41da08fb7c16679430ab79bb'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/readline.cpython-314-darwin.so', '3455c38eb76496eab16c51dd7c6578c07bea4c3cb42044a52119eb02e54a28fd'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/resource.cpython-314-darwin.so', 'e553921abd45409c21f04a06366e89d60fc69c664c29c54517522ef8281494ab'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/select.cpython-314-darwin.so', '70dda54edccb22b0ad3c1c2b3794d139f1a79b54b48de232d50b94ad8e1ccb65'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/syslog.cpython-314-darwin.so', '460c76f146d1efb85fc14e760ff7ab01632b2c83e14a43ac335b532e45422dea'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/termios.cpython-314-darwin.so', 'ebf53d8dc647a01cf24a3445fd6e89013f5fffaa1353ee942f19306bb3da9ccd'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/unicodedata.cpython-314-darwin.so', '62d614020c1077cfc7c8b0a2a07ce61ba80b875ace603fe0ec6f2964a8716b1a'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/xxlimited.cpython-314-darwin.so', 'ee9bd43e0bd405829c59848360d7dee5f5b26ba50a5f1168ccf6f391dc9095e7'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/xxlimited_35.cpython-314-darwin.so', 'd420c3da0d895fe5df8a28f80cb94acdba82b0b691c61364aabb617dad8948fa'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/xxsubtype.cpython-314-darwin.so', 'cfa294c88a2cb361660337b960e028da224081ee3962e76cae86c7177405c7e3'), ('/opt/homebrew/Cellar/python@3.14/3.14.8_2/Frameworks/Python.framework/Versions/3.14/lib/python3.14/lib-dynload/zlib.cpython-314-darwin.so', 'e4de600f58975f92989604cbb2b4769fea3303f8a8bf696e99af527d5e3ab601'), ('/opt/homebrew/Cellar/sqlite/3.54.0/lib/libsqlite3.3.54.0.dylib', 'cbd373e61913b2990f6e5eb2cf06a9ad7e33b8192948297152dbf1a10bcc0205'), ('/opt/homebrew/Cellar/xz/5.8.4/lib/liblzma.5.dylib', '1b2e12cc1b920264f3dd15eeba22e85b8afd589595d8945d1d12a52afa694b09'), ('/opt/homebrew/Cellar/zstd/1.5.7_1/lib/libzstd.1.5.7.dylib', 'f07bd1a16d06bc648ba2111516487cb6156abacbcc14f995228fcd82fc5dc2d1'))

def _bounds(selection):
    for name,maximum in (('maximum_files',50000),('maximum_bytes',2*1024**3),('closure_deadline_seconds',60)):
        value=getattr(selection,name)
        if type(value)is not int or not 1<=value<=maximum:raise ValueError('Explicit finite runtime closure capacity required')

def _private(path,*,empty=False):
    if type(path)is not type(Path('/')) or path.resolve()!=path or path.is_symlink():raise PermissionError('Canonical private runtime directory required')
    value=path.lstat()
    if not stat.S_ISDIR(value.st_mode) or value.st_uid!=os.getuid() or stat.S_IMODE(value.st_mode)!=0o700:
        raise PermissionError('Original private runtime directory required')
    if empty and next(path.iterdir(),None)is not None:raise PermissionError('Explicit empty extension directory required')
    return (value.st_dev,value.st_ino,value.st_uid,value.st_mode)

def _ambient_netrc_paths():
    # Metadata-only refusal, never read credential contents or redirect HOME.
    host_directory=Path(pwd.getpwuid(os.getuid()).pw_dir)
    return (host_directory/'.netrc',host_directory/'_netrc')

def _host_identity():
    user=pwd.getpwuid(os.getuid());host_directory=Path(user.pw_dir)
    if host_directory.resolve()!=host_directory or host_directory.is_symlink():
        raise PermissionError('Canonical OS user directory required')
    info=host_directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=user.pw_uid:
        raise PermissionError('Original OS user directory identity required')
    return (os.getuid(),user.pw_uid,str(host_directory),info.st_dev,info.st_ino,info.st_uid,info.st_mode)

@dataclass(frozen=True)
class FabricCliRuntimeSelection:
    profile:str
    maximum_files:int
    maximum_bytes:int
    closure_deadline_seconds:int
    temporary_directory:Path
    extensions_directory:Path
    def __post_init__(self):
        if type(self.profile)is not str or self.profile!='homebrew-azure-cli-2.90.0-python-3.14.8':raise ValueError('Named selected CLI profile required')
        _bounds(self)
        for path in (self.temporary_directory,self.extensions_directory):
            if type(path)is not type(Path('/')) or not path.is_absolute():raise ValueError('Explicit runtime directories required')

@dataclass(frozen=True)
class FabricHttpRuntimeSelection:
    profile:str
    maximum_files:int
    maximum_bytes:int
    closure_deadline_seconds:int
    ca_bundle:Path
    def __post_init__(self):
        if type(self.profile)is not str or self.profile!='stdlib-python-3.14.8-fabric-http':raise ValueError('Named selected HTTP profile required')
        _bounds(self)
        if type(self.ca_bundle)is not type(Path('/')) or self.ca_bundle!=CA_BUNDLE:raise ValueError('Explicit supported CA selection required')

@dataclass(frozen=True)
class FabricInertCliRuntimeSelection:
    root:Path
    executable:Path
    environment:tuple

@dataclass(frozen=True)
class FabricLoopbackRuntimeSelection:
    interpreter:Path
    port:int

@dataclass(frozen=True)
class _RuntimeOriginal:
    selection:object
    selected:tuple
    roots:tuple
    extra:tuple
    links:tuple
    directories:tuple
    environment:tuple
    command_prefix:tuple
    limits:tuple
    original:tuple
    host_identity:object
    configuration:object
    kind:type
    extension_directory:object

class FabricRuntimeCustody:
    """Full streamed content/inventory/link correspondence, no permission cache."""
    def __setattr__(self,name,value):
        if name=='_anchor' and hasattr(self,name):raise AttributeError('Original runtime witness is immutable')
        object.__setattr__(self,name,value)
    def __init__(self,selection,*,configuration=None,deadline=None):
        if type(selection)not in (FabricCliRuntimeSelection,FabricHttpRuntimeSelection,FabricInertCliRuntimeSelection):raise ValueError('Exact runtime selection required')
        self.selection=selection;self.closed=False;self.configuration=configuration;self.deadline=deadline
        self.directory_pins=();self.host_identity=None
        if type(selection)is FabricInertCliRuntimeSelection:
            if (type(selection.root)is not type(Path('/')) or selection.root.resolve()!=selection.root
                    or type(selection.executable)is not type(Path('/')) or not selection.executable.is_relative_to(selection.root)
                    or type(selection.environment)is not tuple
                    or any(type(p)is not tuple or len(p)!=2 or any(type(v)is not str for v in p)for p in selection.environment)):
                raise ValueError('Exact inert runtime selection required')
            self.roots=(selection.root,);self.extra=();self.maximum_files=128;self.maximum_bytes=1048576;self.scan_seconds=10
            self.links=();self.environment=dict(selection.environment);self.command_prefix=(str(selection.executable),)
        else:
            selection.__post_init__()
            if sys.platform!='darwin' or platform.machine()!='arm64':raise PermissionError('Selected Darwin arm64 runtime required')
            self.maximum_files=selection.maximum_files;self.maximum_bytes=selection.maximum_bytes;self.scan_seconds=selection.closure_deadline_seconds
            cli=type(selection)is FabricCliRuntimeSelection
            self.roots=(CLI_ROOT,PYTHON_ROOT)if cli else(PYTHON_ROOT,)
            selected=NATIVE_DEPENDENCIES if cli else tuple(pair for pair in NATIVE_DEPENDENCIES if '/libsodium/'not in pair[1] and '/libyaml/'not in pair[1] and '/openssl@3/'not in pair[1])
            self.extra=tuple(Path(target)for _,target in selected)+(CA_BUNDLE,)
            self.links=tuple((Path(name),Path(target))for name,target in selected)+((CLI_PYTHON,HTTP_PYTHON),)if cli else tuple((Path(name),Path(target))for name,target in selected)
            self.command_prefix=(str(CLI_PYTHON),'-I','-B','-m','azure.cli')if cli else(str(HTTP_PYTHON),'-I','-S','-B')
            self.environment={'PATH':'/usr/bin:/bin','SSL_CERT_FILE':str(CA_BUNDLE),'REQUESTS_CA_BUNDLE':str(CA_BUNDLE),'__CF_USER_TEXT_ENCODING':'0x%X:0x0:0x0'%os.getuid()}
            if cli:
                self.host_identity=_host_identity()
                if configuration is None:raise ValueError('Explicit original CLI configuration required')
                paths=(configuration,selection.temporary_directory,selection.extensions_directory)
                self.directory_pins=tuple((path,_private(path,empty=path==selection.extensions_directory))for path in paths)
                self.environment.update({'AZURE_CONFIG_DIR':str(configuration),'TMPDIR':str(selection.temporary_directory),'AZURE_CORE_COLLECT_TELEMETRY':'no','AZURE_EXTENSION_USE_DYNAMIC_INSTALL':'no','AZURE_EXTENSION_DIR':str(selection.extensions_directory),'AZURE_EXTENSION_SYS_DIR':str(selection.extensions_directory),'AZURE_EXTENSION_DEV_SOURCES':''})
                # Only noncredential startup configuration; credential caches remain tool-owned.
                self.extra+=tuple(path for path in (configuration/'config',configuration/'clouds.config')if path.exists())
        self.selected=(selection,repr(selection));self.original=self._scan()
        if type(selection)is not FabricInertCliRuntimeSelection:
            pins=PROVENANCE_PINS if type(selection)is FabricCliRuntimeSelection else(PROVENANCE_PINS[-1],)
            native=tuple(pair for pair in NATIVE_PINS if any(Path(pair[0]).is_relative_to(root)for root in self.roots) or Path(pair[0])in self.extra)
            expected_native={path for path,_ in native}
            current_native={row[1]for row in self.original if row[0]=='file'and Path(row[1]).suffix in ('.so','.dylib','.bundle')}
            if not current_native.issubset(expected_native):raise PermissionError('Unselected native runtime module')
            for path,digest in pins+native:
                matching=[row for row in self.original if row[0]=='file' and row[1]==path]
                if len(matching)!=1 or matching[0][-1]!=digest:raise PermissionError('Selected operator-trusted installed profile differs')
        self._anchor=_RuntimeOriginal(selection,self.selected,self.roots,self.extra,self.links,self.directory_pins,tuple(sorted(self.environment.items())),self.command_prefix,(self.maximum_files,self.maximum_bytes,self.scan_seconds,self.deadline),self.original,self.host_identity,self.configuration,type(selection),getattr(selection,'extensions_directory',None))

    def _scan(self,original=None):
        roots=self.roots if original is None else original.roots
        extra=self.extra if original is None else original.extra
        links=self.links if original is None else original.links
        directories=self.directory_pins if original is None else original.directories
        limits=(self.maximum_files,self.maximum_bytes,self.scan_seconds,self.deadline)if original is None else original.limits
        maximum_files,maximum_bytes,scan_seconds,owner_deadline=limits
        kind=type(self.selection)if original is None else original.kind
        configuration=self.configuration if original is None else original.configuration
        extension_directory=getattr(self.selection,'extensions_directory',None)if original is None else original.extension_directory
        host_identity=self.host_identity if original is None else original.host_identity
        deadline=time.monotonic()+scan_seconds
        if owner_deadline is not None:deadline=min(deadline,owner_deadline)
        result=[];seen=set();total=0
        if kind is FabricCliRuntimeSelection:
            if _host_identity()!=host_identity:raise PermissionError('Original OS user directory changed')
            for path in _ambient_netrc_paths():
                try:path.lstat()
                except FileNotFoundError:continue
                raise PermissionError('Ambient credential fallback is outside selected profile')
        def timed():
            if time.monotonic()>=deadline:raise TimeoutError('Runtime closure deadline exceeded')
        def facts(s):return(s.st_dev,s.st_ino,s.st_uid,s.st_mode,s.st_nlink,s.st_size,s.st_mtime_ns)
        def link_chain(path):
            for part in (path,*path.parents):
                timed()
                if part.is_symlink():
                    info=part.lstat();target=os.readlink(part);result.append(('link',str(part),facts(info),target))
        for path,expected in links:
            link_chain(path)
            if path.resolve()!=expected:raise PermissionError('Selected native/venv link target differs')
        for path,original in directories:
            if _private(path,empty=path==extension_directory)!=original:raise PermissionError('Original configuration directory changed')
        candidates=[]
        for root in roots:
            timed()
            if root.resolve()!=root or root.is_symlink() or not root.is_dir():raise PermissionError('Selected runtime root changed')
            candidates.append(root)
            for path in root.rglob('*'):
                timed();candidates.append(path)
                if len(candidates)>maximum_files:raise PermissionError('Runtime inventory capacity exceeded')
        candidates.extend(extra)
        if configuration is not None:
            result.append(('configuration-inventory',tuple((name,(configuration/name).exists())for name in ('config','clouds.config'))))
        for path in sorted(set(candidates)):
            timed()
            info=path.lstat()
            if path.is_symlink():
                target=path.resolve()
                if kind is FabricInertCliRuntimeSelection or not any(target.is_relative_to(root)for root in roots) and target not in extra:
                    raise PermissionError('Unselected runtime link')
                result.append(('link',str(path),facts(info),os.readlink(path)));continue
            if path.is_dir():result.append(('directory',str(path),(info.st_dev,info.st_ino,info.st_uid,info.st_mode)));continue
            if path.resolve()!=path or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1:raise PermissionError('Runtime regular-file custody differs')
            if str(path)in seen:continue
            seen.add(str(path));total+=info.st_size
            if len(result)>maximum_files or total>maximum_bytes:raise PermissionError('Runtime closure capacity exceeded')
            digest=hashlib.sha256();count=0;fd=None;primary=None
            try:
                fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW);before=os.fstat(fd)
                while True:
                    timed();block=os.read(fd,min(65536,info.st_size-count+1))
                    if not block:break
                    count+=len(block)
                    if count>info.st_size:raise PermissionError('Runtime file grew')
                    digest.update(block)
                after=os.fstat(fd);named=path.lstat()
                if facts(info)!=facts(before) or facts(info)!=facts(after) or facts(info)!=facts(named) or count!=info.st_size:
                    raise PermissionError('Runtime file changed while read')
                result.append(('file',str(path),facts(info),digest.hexdigest()))
            except BaseException as error:primary=error
            finish(primary,[]if fd is None else[lambda fd=fd:os.close(fd)])
        return tuple(result)

    def renew(self):
        original=self._anchor
        def inputs():
            current=(self.roots,self.extra,self.links,self.directory_pins,tuple(sorted(self.environment.items())),self.command_prefix,(self.maximum_files,self.maximum_bytes,self.scan_seconds,self.deadline),self.original,self.host_identity,self.configuration)
            expected=(original.roots,original.extra,original.links,original.directories,original.environment,original.command_prefix,original.limits,original.original,original.host_identity,original.configuration)
            if self.closed or self.selection is not original.selection or (self.selection,repr(self.selection))!=original.selected or current!=expected:
                raise PermissionError('Original runtime owning inputs changed')
        def original_content():
            if self._scan(original)!=original.original:raise PermissionError('Original complete runtime correspondence changed')
        finish(None,[inputs,original_content])
    def close(self):
        try:self.renew()
        finally:self.closed=True

@contextmanager
def hold_fabric_cli_runtime(selection,*,configuration=None,deadline=None):
    owner=FabricRuntimeCustody(selection,configuration=configuration,deadline=deadline);primary=None
    try:yield owner
    except BaseException as error:primary=error
    finish(primary,[owner.close])
