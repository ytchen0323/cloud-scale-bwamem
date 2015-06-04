/*******************************************************************************
Vendor: Xilinx 
Associated Filename: test-cl.c
Purpose: OpenCL Host Code for Matrix Multiply Example
Revision History: July 1, 2013 - initial release
                                                
*******************************************************************************
Copyright (C) 2013 XILINX, Inc.

This file contains confidential and proprietary information of Xilinx, Inc. and 
is protected under U.S. and international copyright and other intellectual 
property laws.

DISCLAIMER
This disclaimer is not a license and does not grant any rights to the materials 
distributed herewith. Except as otherwise provided in a valid license issued to 
you by Xilinx, and to the maximum extent permitted by applicable law: 
(1) THESE MATERIALS ARE MADE AVAILABLE "AS IS" AND WITH ALL FAULTS, AND XILINX 
HEREBY DISCLAIMS ALL WARRANTIES AND CONDITIONS, EXPRESS, IMPLIED, OR STATUTORY, 
INCLUDING BUT NOT LIMITED TO WARRANTIES OF MERCHANTABILITY, NON-INFRINGEMENT, OR 
FITNESS FOR ANY PARTICULAR PURPOSE; and (2) Xilinx shall not be liable (whether 
in contract or tort, including negligence, or under any other theory of 
liability) for any loss or damage of any kind or nature related to, arising under 
or in connection with these materials, including for any direct, or any indirect, 
special, incidental, or consequential loss or damage (including loss of data, 
profits, goodwill, or any type of loss or damage suffered as a result of any 
action brought by a third party) even if such damage or loss was reasonably 
foreseeable or Xilinx had been advised of the possibility of the same.

CRITICAL APPLICATIONS
Xilinx products are not designed or intended to be fail-safe, or for use in any 
application requiring fail-safe performance, such as life-support or safety 
devices or systems, Class III medical devices, nuclear facilities, applications 
related to the deployment of airbags, or any other applications that could lead 
to death, personal injury, or severe property or environmental damage 
(individually and collectively, "Critical Applications"). Customer assumes the 
sole risk and liability of any use of Xilinx products in Critical Applications, 
subject only to applicable laws and regulations governing limitations on product 
liability. 

THIS COPYRIGHT NOTICE AND DISCLAIMER MUST BE RETAINED AS PART OF THIS FILE AT 
ALL TIMES.

*******************************************************************************/
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <assert.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <CL/opencl.h>

//#include "my_socket.h"
#include "my_timer.h"

#define TOTAL_TASK_NUMS 65536
#define DATA_SIZE (TOTAL_TASK_NUMS*64)
#define FPGA_RET_PARAM_NUM 5
#define RESULT_SIZE TOTAL_TASK_NUMS*FPGA_RET_PARAM_NUM

#define DONE 1
#define FLAG_NUM 2

// packet size interms of # of integers
#define PACKET_SIZE 2
#define TIME_BUF_SIZE 8

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <time.h>
#include <inttypes.h>

////////////////////////////////////////////////////////////////////////////////

int
load_file_to_memory(const char *filename, char **result)
{ 
  int size = 0;
  FILE *f = fopen(filename, "rb");
  if (f == NULL) 
  { 
    *result = NULL;
    return -1; // -1 means file opening fail 
  } 
  fseek(f, 0, SEEK_END);
  size = ftell(f);
  fseek(f, 0, SEEK_SET);
  *result = (char *)malloc(size+1);
  if (size != fread(*result, sizeof(char), size, f)) 
  { 
    free(*result);
    return -2; // -2 means file reading fail 
  } 
  fclose(f);
  (*result)[size] = 0;
  return size;
}


void collect_timer_stats(
      int ConnectFD,
      timespec* socListenTime, 
      timespec* socSendTime,
      timespec* socRecvTime,
      timespec* exeTime,
      timespec* timer)
{
  int time_buf[TIME_BUF_SIZE];  

  printf("**********   collect socket timing information and reset timer   **********\n");
  printTimeSpec(*socListenTime);
  printTimeSpec(*socSendTime);
  printTimeSpec(*socRecvTime);
  printTimeSpec(*exeTime);
  printf("**********   end   **********\n\n");
  time_buf[0] = socListenTime->tv_sec;
  time_buf[1] = socListenTime->tv_nsec;
  time_buf[2] = socSendTime->tv_sec;
  time_buf[3] = socSendTime->tv_nsec;
  time_buf[4] = socRecvTime->tv_sec;
  time_buf[5] = socRecvTime->tv_nsec;
  time_buf[6] = exeTime->tv_sec;
  time_buf[7] = exeTime->tv_nsec;
  send(ConnectFD, time_buf, TIME_BUF_SIZE * sizeof(int), 0); 
  close(ConnectFD);
  *socListenTime = diff(*timer, *timer);
  *socSendTime = diff(*timer, *timer);
  *socRecvTime = diff(*timer, *timer);
  *exeTime = diff(*timer, *timer);
  printf("**********   after timer reset  **********\n");
  printTimeSpec(*socListenTime);
  printTimeSpec(*socSendTime);
  printTimeSpec(*socRecvTime);
  printTimeSpec(*exeTime);
  printf("**********   end   **********\n\n");
}

void print_current_time_with_ns (void)
{
  long            ns; // Milliseconds
  //time_t          s;  // Seconds
  struct timespec spec;

  clock_gettime(CLOCK_REALTIME, &spec);

  //s  = spec.tv_sec;
  ns = spec.tv_nsec; // Convert nanoseconds to milliseconds

  printf("Current time: %ld nanoseconds since the Epoch\n", ns);
}


int main(int argc, char** argv)
{
  int err;                            // error code returned from api calls     
  int* a = (int*) malloc(DATA_SIZE * sizeof(int));                   // original data set given to device
  int* results = (int*) malloc(RESULT_SIZE * sizeof(int));           // results returned from device
  unsigned int correct;               // number of correct results returned

  size_t global[2];                   // global domain size for our calculation
  size_t local[2];                    // local domain size for our calculation

  cl_platform_id platform_id;         // platform id
  cl_device_id device_id;             // compute device id 
  cl_context context;                 // compute context
  cl_command_queue commands;          // compute command queue
  cl_program program;                 // compute program
  cl_kernel kernel;                   // compute kernel
   
  char cl_platform_vendor[1001];
  char cl_platform_name[1001];
   
  cl_mem input_a;                     // device memory used for the input array
  //cl_mem input_b;                     // device memory used for the input array
  cl_mem output;                      // device memory used for the output array
  int inc;
  double t_start, t_end;

  if (argc != 2){
    printf("%s <inputfile>\n", argv[0]);
    return EXIT_FAILURE;
  }

  // Connect to first platform
  //
  err = clGetPlatformIDs(1,&platform_id,NULL);
  if (err != CL_SUCCESS)
  {
    printf("Error: Failed to find an OpenCL platform!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  err = clGetPlatformInfo(platform_id,CL_PLATFORM_VENDOR,1000,(void *)cl_platform_vendor,NULL);
  if (err != CL_SUCCESS)
  {
    printf("Error: clGetPlatformInfo(CL_PLATFORM_VENDOR) failed!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  printf("CL_PLATFORM_VENDOR %s\n",cl_platform_vendor);
  err = clGetPlatformInfo(platform_id,CL_PLATFORM_NAME,1000,(void *)cl_platform_name,NULL);
  if (err != CL_SUCCESS)
  {
    printf("Error: clGetPlatformInfo(CL_PLATFORM_NAME) failed!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  printf("CL_PLATFORM_NAME %s\n",cl_platform_name);
 
  // Connect to a compute device
  //
  int fpga = 0;
#if defined (FPGA_DEVICE)
  fpga = 1;
#endif
  err = clGetDeviceIDs(platform_id, fpga ? CL_DEVICE_TYPE_ACCELERATOR : CL_DEVICE_TYPE_CPU,
                       1, &device_id, NULL);
  if (err != CL_SUCCESS)
  {
    printf("Error: Failed to create a device group!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  
  // Create a compute context 
  //
  context = clCreateContext(0, 1, &device_id, NULL, NULL, &err);
  if (!context)
  {
    printf("Error: Failed to create a compute context!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }

  // Create a command commands
  //
  commands = clCreateCommandQueue(context, device_id, 0, &err);
  if (!commands)
  {
    printf("Error: Failed to create a command commands!\n");
    printf("Error: code %i\n",err);
    printf("Test failed\n");
    return EXIT_FAILURE;
  }

  int status;

  // Create Program Objects
  //
  
  // Load binary from disk
  unsigned char *kernelbinary;
  char *xclbin=argv[1];
  printf("loading %s\n", xclbin);
  int n_i = load_file_to_memory(xclbin, (char **) &kernelbinary);
  if (n_i < 0) {
    printf("failed to load kernel from xclbin: %s\n", xclbin);
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  else {
	  printf("Succeed to load kernel from xclbin: %s\n", xclbin);
  }
  size_t n = n_i;
  // Create the compute program from offline
  program = clCreateProgramWithBinary(context, 1, &device_id, &n,
                                      (const unsigned char **) &kernelbinary, &status, &err);
  if ((!program) || (err!=CL_SUCCESS)) {
    printf("Error: Failed to create compute program from binary %d!\n", err);
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  else {
	  printf("Succeed to create compute program from binary %d!\n", err);
  }

  // Build the program executable
  //
  err = clBuildProgram(program, 0, NULL, NULL, NULL, NULL);
  if (err != CL_SUCCESS)
  {
    size_t len;
    char buffer[2048];

    printf("Error: Failed to build program executable!\n");
    clGetProgramBuildInfo(program, device_id, CL_PROGRAM_BUILD_LOG, sizeof(buffer), buffer, &len);
    printf("%s\n", buffer);
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  else {
	  printf("Succeed to build program executable!\n");
  }

  // Create the compute kernel in the program we wish to run
  //
  kernel = clCreateKernel(program, "mmult", &err);
  if (!kernel || err != CL_SUCCESS)
  {
    printf("Error: Failed to create compute kernel!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }
  else {
	  printf("Succeed to create compute kernel!\n");
  }

  // Create the input and output arrays in device memory for our calculation
  //
  input_a = clCreateBuffer(context,  CL_MEM_READ_ONLY,  sizeof(int) * DATA_SIZE, NULL, NULL);
  output = clCreateBuffer(context, CL_MEM_WRITE_ONLY, sizeof(int) * RESULT_SIZE, NULL, NULL);
  if (!input_a || !output)
  {
    printf("Error: Failed to allocate device memory!\n");
    printf("Test failed\n");
    return EXIT_FAILURE;
  }    
  else {
	  printf("Succeed to allocate device memory!\n");
  }

  // set up socket
  printf("\n************* Welcome to UCLA FPGA agent! **********\n");
  struct sockaddr_in stSockAddr;
  int SocketFD = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);

  if(-1 == SocketFD) {
    perror("can not create socket");
    exit(EXIT_FAILURE);
  }

  memset(&stSockAddr, 0, sizeof(stSockAddr));

  stSockAddr.sin_family = AF_INET;
  stSockAddr.sin_port = htons(7000);
  stSockAddr.sin_addr.s_addr = htonl(INADDR_ANY);

  if(-1 == bind(SocketFD,(struct sockaddr *)&stSockAddr, sizeof(stSockAddr))) {
    perror("error bind failed");
    close(SocketFD);
    exit(EXIT_FAILURE);
  }

  if(-1 == listen(SocketFD, 10)) {
    perror("error listen failed");
    close(SocketFD);
    exit(EXIT_FAILURE);
  }


  int taskNum = -1;

  // polling setting
  timespec deadline;
  deadline.tv_sec = 0;
  deadline.tv_nsec = 100;

  // Get the start time
  timespec timer = tic( );
  timespec socListenTime = diff(timer, timer);
  timespec socSendTime = diff(timer, timer);
  timespec socRecvTime = diff(timer, timer);
  timespec exeTime = diff(timer, timer);

  bool broadcastFlag = false;

  int packet_buf[PACKET_SIZE];
  int time_buf[TIME_BUF_SIZE];

  while (true) {
    //printf("\n************* Got a new task! *************\n");
    timer = tic();

    int ConnectFD = accept(SocketFD, NULL, NULL);
    if (!broadcastFlag) { 
        broadcastFlag = true;
        timer = tic();
    }

    // For profiling only
    //struct timeval  tv;
    //gettimeofday(&tv, NULL);    
    //double time_in_mill = (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000 ; // convert tv_sec & tv_usec to millisecond
    //printf("Receive time (ms): %lf\n", time_in_mill);
    print_current_time_with_ns();

    accTime (&socListenTime, &timer);

    if(0 > ConnectFD) {
      perror("error accept failed");
      close(SocketFD);
      exit(EXIT_FAILURE);
    }    
    
    read(ConnectFD, &packet_buf, PACKET_SIZE * sizeof(int));

    // send FPGA stats back to java application
    if(packet_buf[0] == -1) {
      // for profiling use
      collect_timer_stats(ConnectFD, &socListenTime, &socSendTime, &socRecvTime, &exeTime, &timer);
      broadcastFlag = false;
      continue;
    }

    char* shm_addr;
    int shmid = -1;
    int data_size = -1;  // data sent to FPGA (unit: int)
    shmid = packet_buf[0];
    data_size = packet_buf[1];
    printf("Shmid: %d, Data size (# of int): %d\n", shmid, data_size);

    // shared memory
    if((shm_addr = (char *) shmat(shmid, NULL, 0)) == (char *) -1) {
      perror("Server: shmat failed.");
      exit(1);
    }
    //else
      //printf("Server: attach shared memory: %p\n", shm_addr);

    int done = 0;
    while(done == 0) {
      done = (int) *((int*)shm_addr);
      clock_nanosleep(CLOCK_REALTIME, 0, &deadline, NULL);
    }

    //printf("Copy data to the array in the host\n");
    memcpy(a, shm_addr + FLAG_NUM * sizeof(int), sizeof(int) * data_size);
    
    accTime (&socSendTime, &timer);

    taskNum = a[2];
    for (int i=0; i<taskNum; i++) {
      int tmp = *(a+8+i*8+7);
      assert(tmp >=0 && tmp < TOTAL_TASK_NUMS);
    }
    printf("Task Num: %d\n", taskNum);

    //printf("\nparameter recieved --- \n");
    //Write our data set into the input array in device memory 
    
    //printf("Write data from host to FPGA\n");
    err = clEnqueueWriteBuffer(commands, input_a, CL_TRUE, 0, sizeof(int) * data_size, a, 0, NULL, NULL);
    if (err != CL_SUCCESS)
    {
      printf("Error: Failed to write to source array a!\n");
      printf("Test failed\n");
      return EXIT_FAILURE;
    }
      
    // Set the arguments to our compute kernel
    //
    err = 0;
    err  = clSetKernelArg(kernel, 0, sizeof(cl_mem), &input_a);
    err |= clSetKernelArg(kernel, 1, sizeof(cl_mem), &output);
    err |= clSetKernelArg(kernel, 2, sizeof(int), &taskNum);
    if (err != CL_SUCCESS)
    {
      printf("Error: Failed to set kernel arguments! %d\n", err);
      printf("Test failed\n");
      return EXIT_FAILURE;
    }
  
    // Execute the kernel over the entire range of our 1d input data set
    // using the maximum number of work group items for this device
    //
  
    //printf("Enqueue Task\n");
    err = clEnqueueTask(commands, kernel, 0, NULL, NULL);
    if (err)
    {
      printf("Error: Failed to execute kernel! %d\n", err);
      printf("Test failed\n");
      return EXIT_FAILURE;
    }
  
    // Read back the results from the device to verify the output
    //
    cl_event readevent;
    //printf("Enqueue read buffer\n");
    err = clEnqueueReadBuffer( commands, output, CL_TRUE, 0, sizeof(int) * FPGA_RET_PARAM_NUM * taskNum, results, 0, NULL, &readevent );  
    if (err != CL_SUCCESS)
    {
      printf("Error: Failed to read output array! %d\n", err);
      printf("Test failed\n");
      return EXIT_FAILURE;
    }
  
    //printf("Wait for FPGA results\n");
    clWaitForEvents(1, &readevent);
    accTime(&exeTime, &timer);
  
    // Get the execution time
    //toc(&timer);

    // put data back to shared memory
    //printf("Put data back to the shared memory\n");
    memcpy(shm_addr + FLAG_NUM * sizeof(int), results, sizeof(int) * FPGA_RET_PARAM_NUM * taskNum);
    *((int*)(shm_addr + sizeof(int))) = DONE;

    //printf("\n************* Task finished! *************\n");

    if (-1 == shutdown(ConnectFD, SHUT_RDWR)) {
      perror("can not shutdown socket");
      close(ConnectFD);
      close(SocketFD);
      exit(EXIT_FAILURE);
    }
    close(ConnectFD);

    //printf("done\n");

    // free the shared memory
    shmdt(shm_addr);
    //shmctl(shmid, IPC_RMID, 0);

    accTime(&socRecvTime, &timer);

    printf("**********timing begin**********\n");
    printTimeSpec(socListenTime);
    printTimeSpec(socSendTime);
    printTimeSpec(socRecvTime);
    printTimeSpec(exeTime);
    printf("**********timing end**********\n\n");
  }
    
  close(SocketFD);

  // Shutdown and cleanup
  //
  clReleaseMemObject(input_a);
  clReleaseMemObject(output);
  clReleaseProgram(program);
  clReleaseKernel(kernel);
  clReleaseCommandQueue(commands);
  clReleaseContext(context);

  return EXIT_SUCCESS;

}
