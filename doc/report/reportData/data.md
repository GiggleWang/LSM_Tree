| 总次数     | PUT     | GET     | DELETE  |
| ---------- | ------- | ------- | ------- |
| 8 * 1024   | 13430   | 44884.5 | 9757.39 |
| 16 * 1024  | 7779.19 | 18247.3 | 5400.26 |
| 32 * 1024  | 4101.77 | 9562.46 | 2881.71 |
| 64 * 1024  | 2070.52 | 4483.21 | 1157.59 |
| 128 * 1024 | 1078.45 | 2107.59 | 914.298 |
| 256 * 1024 | 558.513 | 1128.48 | 432.926 |

number = 256 * 1024测试

0-0-0：14066.2

0-0-1:   1031.39

0-1-1:   1005.04



```
(base) wyx@wyx:~/study/projects/ADS/LSMtree$ ./test 
Total elapsed time of get from 0 to 4096: 8.879e-06 seconds
Throughput: 4.61313e+08 operations per second
Total elapsed time of get from 4096 to 8192: 0.00340925 seconds
Throughput: 1.20144e+06 operations per second
Total elapsed time of get from 8192 to 12288: 0.00342014 seconds
Throughput: 1.19761e+06 operations per second
Total elapsed time of get from 12288 to 16384: 0.0034307 seconds
Throughput: 1.19392e+06 operations per second
Total elapsed time of get from 16384 to 20480: 0.00344102 seconds
Throughput: 1.19035e+06 operations per second
Total elapsed time of get from 20480 to 24576: 0.00345123 seconds
Throughput: 1.18682e+06 operations per second
Total elapsed time of get from 24576 to 28672: 0.00346144 seconds
Throughput: 1.18332e+06 operations per second
Total elapsed time of get from 28672 to 32768: 0.00347164 seconds
Throughput: 1.17985e+06 operations per second
Total elapsed time of get from 32768 to 36864: 0.00348217 seconds
Throughput: 1.17628e+06 operations per second
Total elapsed time of get from 36864 to 40960: 0.00349239 seconds
Throughput: 1.17284e+06 operations per second
Total elapsed time of get from 40960 to 45056: 0.0035026 seconds
Throughput: 1.16942e+06 operations per second
Total elapsed time of get from 45056 to 49152: 0.00351276 seconds
Throughput: 1.16603e+06 operations per second
Total elapsed time of get from 49152 to 53248: 0.00352322 seconds
Throughput: 1.16257e+06 operations per second
Total elapsed time of get from 53248 to 57344: 0.00353388 seconds
Throughput: 1.15907e+06 operations per second
Total elapsed time of get from 57344 to 61440: 0.00354447 seconds
Throughput: 1.1556e+06 operations per second
Total elapsed time of get from 61440 to 65536: 0.00355475 seconds
Throughput: 1.15226e+06 operations per second
Total elapsed time of get from 65536 to 69632: 0.00356499 seconds
Throughput: 1.14895e+06 operations per second
Total elapsed time of get from 69632 to 73728: 0.00357524 seconds
Throughput: 1.14566e+06 operations per second
Total elapsed time of get from 73728 to 77824: 0.00358543 seconds
Throughput: 1.1424e+06 operations per second
Total elapsed time of get from 77824 to 81920: 0.00359563 seconds
Throughput: 1.13916e+06 operations per second
Total elapsed time of get from 81920 to 86016: 0.00360648 seconds
Throughput: 1.13573e+06 operations per second
Total elapsed time of get from 86016 to 90112: 0.00361713 seconds
Throughput: 1.13239e+06 operations per second
Total elapsed time of get from 90112 to 94208: 0.00362737 seconds
Throughput: 1.12919e+06 operations per second
Total elapsed time of get from 94208 to 98304: 0.00363758 seconds
Throughput: 1.12602e+06 operations per second
Total elapsed time of get from 98304 to 102400: 0.0036478 seconds
Throughput: 1.12287e+06 operations per second
Total elapsed time of get from 102400 to 106496: 0.00365802 seconds
Throughput: 1.11973e+06 operations per second
Total elapsed time of get from 106496 to 110592: 0.00367026 seconds
Throughput: 1.116e+06 operations per second
Total elapsed time of get from 110592 to 114688: 0.00368092 seconds
Throughput: 1.11277e+06 operations per second
Total elapsed time of get from 114688 to 118784: 0.00369156 seconds
Throughput: 1.10956e+06 operations per second
Total elapsed time of get from 118784 to 122880: 0.00370211 seconds
Throughput: 1.1064e+06 operations per second
Total elapsed time of get from 122880 to 126976: 0.00371265 seconds
Throughput: 1.10326e+06 operations per second
Total elapsed time of get from 126976 to 131072: 0.00372347 seconds
Throughput: 1.10005e+06 operations per second
Total elapsed time of get from 131072 to 135168: 0.00373466 seconds
Throughput: 1.09675e+06 operations per second
Total elapsed time of get from 135168 to 139264: 0.0037453 seconds
Throughput: 1.09364e+06 operations per second
Total elapsed time of get from 139264 to 143360: 0.00375586 seconds
Throughput: 1.09056e+06 operations per second
Total elapsed time of get from 143360 to 147456: 0.00376632 seconds
Throughput: 1.08753e+06 operations per second
Total elapsed time of get from 147456 to 151552: 0.00377692 seconds
Throughput: 1.08448e+06 operations per second
Total elapsed time of get from 151552 to 155648: 0.00378737 seconds
Throughput: 1.08149e+06 operations per second
Total elapsed time of get from 155648 to 159744: 0.00379788 seconds
Throughput: 1.0785e+06 operations per second
Total elapsed time of get from 159744 to 163840: 0.0038081 seconds
Throughput: 1.0756e+06 operations per second
Total elapsed time of get from 163840 to 167936: 0.00381833 seconds
Throughput: 1.07272e+06 operations per second
Total elapsed time of get from 167936 to 172032: 0.00382852 seconds
Throughput: 1.06986e+06 operations per second
Total elapsed time of get from 172032 to 176128: 0.00383872 seconds
Throughput: 1.06702e+06 operations per second
Total elapsed time of get from 176128 to 180224: 0.00384927 seconds
Throughput: 1.0641e+06 operations per second
Total elapsed time of get from 180224 to 184320: 0.00385983 seconds
Throughput: 1.06119e+06 operations per second
Total elapsed time of get from 184320 to 188416: 0.00387041 seconds
Throughput: 1.05829e+06 operations per second
Total elapsed time of get from 188416 to 192512: 0.00388096 seconds
Throughput: 1.05541e+06 operations per second
Total elapsed time of get from 192512 to 196608: 0.00389149 seconds
Throughput: 1.05255e+06 operations per second
Total elapsed time of get from 196608 to 200704: 0.00390199 seconds
Throughput: 1.04972e+06 operations per second
Total elapsed time of get from 200704 to 204800: 0.00391252 seconds
Throughput: 1.0469e+06 operations per second
Total elapsed time of get from 204800 to 208896: 0.00392314 seconds
Throughput: 1.04406e+06 operations per second
Total elapsed time of get from 208896 to 212992: 0.00393377 seconds
Throughput: 1.04124e+06 operations per second
Total elapsed time of get from 212992 to 217088: 0.00394456 seconds
Throughput: 1.03839e+06 operations per second
Total elapsed time of get from 217088 to 221184: 0.0039551 seconds
Throughput: 1.03563e+06 operations per second
Total elapsed time of get from 221184 to 225280: 0.00396565 seconds
Throughput: 1.03287e+06 operations per second
Total elapsed time of get from 225280 to 229376: 0.00397614 seconds
Throughput: 1.03014e+06 operations per second
Total elapsed time of get from 229376 to 233472: 0.00398683 seconds
Throughput: 1.02738e+06 operations per second
Total elapsed time of get from 233472 to 237568: 0.00399731 seconds
Throughput: 1.02469e+06 operations per second
Total elapsed time of get from 237568 to 241664: 0.00400781 seconds
Throughput: 1.02201e+06 operations per second
Total elapsed time of get from 241664 to 245760: 0.0040184 seconds
Throughput: 1.01931e+06 operations per second
Total elapsed time of get from 245760 to 249856: 0.00402914 seconds
Throughput: 1.01659e+06 operations per second
Total elapsed time of get from 249856 to 253952: 0.00403958 seconds
Throughput: 1.01397e+06 operations per second
Total elapsed time of get from 253952 to 258048: 0.00405004 seconds
Throughput: 1.01135e+06 operations per second

```

| BF大小(kb) | Get吞吐量 | Put吞吐量 |
| ---------- | --------- | --------- |
| 1          | 802.30    | 569.45    |
| 4          | 947.89    | 550.57    |
| 8          | 1128.48   | 558.51    |
| 16         | 1110.34   | 563.90    |
| 32         | 1069.47   | 560.35    |
| 64         | 1005.41   | 559.49    |

