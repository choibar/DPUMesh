dpa_program_lib = custom_target('dpa_kernel',
  input : ['dpa_kernel.c', 'benchmarks/dpa_bench.c'],
  output : 'dpa_kernel.a',
  depend_files : files('../common/dpa_common.h', 'hpack_walk.h', 'huff_fsm.h',
                       '../common/dpa_bench.h', 'benchmarks/hpack_term.h', 'benchmarks/dsb_blocks.h'),
  command : [dpacc_build_script, meson.project_build_root(), meson.project_source_root(),
             '@INPUT0@', program_name, dpacc_mcpu_flag, doca_lib_dir, '@INPUT1@'],
  build_by_default : true,
)
