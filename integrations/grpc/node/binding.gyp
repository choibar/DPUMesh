{
  "targets": [
    {
      "target_name": "dpumesh_grpc",
      "sources": ["src/addon.cc"],
      "include_dirs": ["../cpp/include"],
      "libraries": ["-ldl"],
      "cflags_cc": ["-std=c++17", "-Wall", "-Wextra"],
      "cflags_cc!": ["-fno-exceptions"]
    }
  ]
}
