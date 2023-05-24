#pragma once

typedef void* cuda_stream_handle;

typedef void* cuda_event_handle;

extern "C"
int cuda_check(cuda_event_handle eve_hdl);

extern "C"
void cuda_stream_event_record(cuda_stream_handle strm_hdl, cuda_event_handle eve_hdl);

extern "C"
cuda_stream_handle create_cuda_stream();

extern "C"
cuda_event_handle create_cuda_event();


