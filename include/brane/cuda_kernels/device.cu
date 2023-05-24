#include <cuda.h> 
#include <cuda_runtime.h>
#include "device.h"

extern "C"
cuda_stream_handle create_cuda_stream() {
    cudaStream_t retval;
    cudaStreamCreate(&retval);
    return static_cast<cuda_stream_handle>(retval);
}

extern "C"
cuda_event_handle create_cuda_event() {
    cudaEvent_t stop;
    cudaEventCreate(&stop);
    return static_cast<cuda_event_handle>(stop);
}

extern "C" 
void cuda_stream_event_record(cuda_stream_handle strm_hdl, cuda_event_handle eve_hdl){
    cudaEventRecord(static_cast<cudaEvent_t>(eve_hdl), static_cast<cudaStream_t>(strm_hdl));
}

extern "C" 
int cuda_check(cuda_event_handle eve_hdl){
    if (cudaEventQuery(static_cast<cudaEvent_t>(eve_hdl))==cudaErrorNotReady)
       return 0;
    else 
       return 1;
}
