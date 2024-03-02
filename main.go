package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	spec "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	runtimeapiV1alpha2 "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"net"
	"os"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

const (
	ContainerdUnixPath = "/run/containerd/containerd.sock"
	CrioUnixPath       = "/run/crio/crio.sock"
	DockerShimUnixPath = "/var/run/dockershim.sock"
)

var client *Client
var runtimeMtx sync.Mutex

type version int

const (
	versionV1beta = version(1)
	versionV1     = version(2)

	defaultHostMountPrefix = "/host"
)

type ContainerInfo struct {
	SandboxID   string    `json:"sandboxID"`
	Pid         int       `json:"pid"`
	RuntimeSpec spec.Spec `json:"runtimeSpec"`
	Privileged  bool      `json:"privileged"`
}

type containerInfo struct {
	SandboxID   string    `json:"sandboxID"`
	Pid         int       `json:"pid"`
	RuntimeSpec spec.Spec `json:"runtimeSpec"`
	Privileged  bool      `json:"privileged"`
}

type Client struct {
	//runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	runtimeClient         runtimeapi.RuntimeServiceClient
	runtimeClientV1alpha2 runtimeapiV1alpha2.RuntimeServiceClient
	useVersion            version
	closure               func() (uint64, error)
	containerInfoClosure  func() (*ContainerInfo, error)
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func probe(c *Client, ep string) (*Client, error) {
	ret, err := PathExists(ep)
	if !ret {
		return nil, fmt.Errorf("%s is not exist", ep)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", addr)
	}

	conn, err := grpc.DialContext(ctx, ep, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithContextDialer(dial))
	if err != nil {
		// try next endpoint
		fmt.Printf("connect %s error:%s", ep, err)
		return nil, err
	}

	//defer conn.Close()
	c.runtimeClient = runtimeapi.NewRuntimeServiceClient(conn)
	if _, err := c.runtimeClient.Version(ctx, &runtimeapi.VersionRequest{}); err == nil {
		fmt.Printf("Using CRI v1 runtime API")
		c.useVersion = versionV1

	} else if status.Code(err) == codes.Unimplemented {
		fmt.Printf("Falling back to CRI v1alpha2 runtime API (deprecated)")
		c.runtimeClientV1alpha2 = runtimeapiV1alpha2.NewRuntimeServiceClient(conn)
		c.useVersion = versionV1beta
	} else {
		conn.Close()
		fmt.Printf("%s", fmt.Errorf("unable to determine runtime API version with %q, or API is not implemented: %w", ep, err))
		return nil, err
	}
	return c, nil
}

func newClient(endpoints []string) (*Client, error) {
	c := &Client{}
	for _, ep := range endpoints {
		cli, err := probe(c, ep)
		if err != nil {
			fmt.Printf("runtime probe error:%s", err)
		}
		if cli != nil {
			return cli, nil
		}
	}
	return nil, errors.New("init runtime client failed")
}

func getHostMountPrefixes() []string {
	return []string{""}
}

func InitRuntimeClient() (*Client, error) {
	var err error
	client, err = newClient([]string{
		"/var/run/koord-runtimeproxy/runtimeproxy.sock",
	})
	if err != nil {
		return nil, err
	}

	return client, nil
}

func GetRuntimeClient() *Client {
	if client != nil {
		return client
	}
	runtimeMtx.Lock()
	defer runtimeMtx.Unlock()
	return nil
}

func ByteToStringUnsafe(b []byte) string {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{Data: bh.Data, Len: bh.Len}
	return *(*string)(unsafe.Pointer(&sh))
}

func StringToByteUnsafe(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{Data: sh.Data, Len: sh.Len, Cap: sh.Len}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

func (c *Client) SetTestClosure(testFun func() (uint64, error)) {
	c.closure = testFun
}

func (c *Client) SetContainerInfoTestClosure(testFun func() (*ContainerInfo, error)) {
	c.containerInfoClosure = testFun
}

func (c *Client) GetPid(containerId string) (uint64, error) {
	if c == nil {
		return 0, errors.New("runtime client is nil")
	}
	if c.closure != nil {
		return c.closure()
	}
	switch c.useVersion {
	case versionV1:
		stats, err := c.runtimeClient.ContainerStatus(context.TODO(), &runtimeapi.ContainerStatusRequest{
			ContainerId: containerId,
			Verbose:     true,
		})
		if err != nil {
			return 0, err
		}
		if stats.Info == nil {
			return 0, errors.New(fmt.Sprintf("stats Info is nil;container id :%s", containerId))
		}
		content, ok := stats.Info["info"]
		if ok == false {
			return 0, errors.New(fmt.Sprintf("stats Info is not exist info;container id :%s", containerId))
		}

		var container containerInfo

		err = json.Unmarshal(StringToByteUnsafe(content), &container)
		if err != nil {
			return 0, err
		}
		return uint64(container.Pid), nil
	case versionV1beta:
		stats, err := c.runtimeClientV1alpha2.ContainerStatus(context.TODO(), &runtimeapiV1alpha2.ContainerStatusRequest{
			ContainerId: containerId,
			Verbose:     true,
		})
		if err != nil {
			return 0, err
		}
		if stats.Info == nil {
			return 0, errors.New(fmt.Sprintf("stats Info is nil;container id :%s", containerId))
		}
		content, ok := stats.Info["info"]
		if ok == false {
			return 0, errors.New(fmt.Sprintf("stats Info is not exist info;container id :%s", containerId))
		}

		var container containerInfo

		err = json.Unmarshal(StringToByteUnsafe(content), &container)
		if err != nil {
			return 0, err
		}
		return uint64(container.Pid), nil
	default:
		return 0, errors.New("unSupport version")
	}

}

func (c *Client) All() (interface{}, error) {
	if c == nil {
		return 0, errors.New("runtime client is nil")
	}
	if c.closure != nil {
		return c.closure()
	}
	switch c.useVersion {
	case versionV1:
		all, err := c.runtimeClient.ListContainers(context.TODO(), &runtimeapi.ListContainersRequest{})
		return all, err
	case versionV1beta:
		all, err := c.runtimeClientV1alpha2.ListContainers(context.TODO(), &runtimeapiV1alpha2.ListContainersRequest{})
		return all, err
	default:
		return 0, errors.New("unSupport version")
	}

}

func (c *Client) PreRunPodSandbox() (interface{}, error) {
	if c == nil {
		return 0, errors.New("runtime client is nil")
	}
	if c.closure != nil {
		return c.closure()
	}
	switch c.useVersion {
	case versionV1:
		all, err := c.runtimeClient.RunPodSandbox(context.TODO(), &runtimeapi.RunPodSandboxRequest{})
		return all, err
	case versionV1beta:
		all, err := c.runtimeClientV1alpha2.RunPodSandbox(context.TODO(), &runtimeapiV1alpha2.RunPodSandboxRequest{})
		return all, err
	default:
		return 0, errors.New("unSupport version")
	}

}

func (c *Client) GetContainerInfo(containerId string) (*ContainerInfo, error) {
	if c == nil {
		return nil, errors.New("runtime client is nil")
	}
	if c.containerInfoClosure != nil {
		return c.containerInfoClosure()
	}
	switch c.useVersion {
	case versionV1:
		stats, err := c.runtimeClient.ContainerStatus(context.TODO(), &runtimeapi.ContainerStatusRequest{
			ContainerId: containerId,
			Verbose:     true,
		})
		if err != nil {
			return nil, err
		}
		if stats.Info == nil {
			return nil, errors.New(fmt.Sprintf("stats Info is nil;container id :%s", containerId))
		}
		content, ok := stats.Info["info"]
		if ok == false {
			return nil, errors.New(fmt.Sprintf("stats Info is not exist info;container id :%s", containerId))
		}

		var container ContainerInfo

		err = json.Unmarshal(StringToByteUnsafe(content), &container)
		if err != nil {
			return nil, err
		}
		return &container, nil
	case versionV1beta:
		stats, err := c.runtimeClientV1alpha2.ContainerStatus(context.TODO(), &runtimeapiV1alpha2.ContainerStatusRequest{
			ContainerId: containerId,
			Verbose:     true,
		})
		if err != nil {
			return nil, err
		}
		if stats.Info == nil {
			return nil, errors.New(fmt.Sprintf("stats Info is nil;container id :%s", containerId))
		}
		content, ok := stats.Info["info"]
		if ok == false {
			return nil, errors.New(fmt.Sprintf("stats Info is not exist info;container id :%s", containerId))
		}

		var container ContainerInfo

		err = json.Unmarshal(StringToByteUnsafe(content), &container)
		if err != nil {
			return nil, err
		}
		return &container, nil
	default:
		return nil, errors.New("unSupport version")
	}
}

func main() {
	InitRuntimeClient()
	all, err := GetRuntimeClient().PreRunPodSandbox()
	if err != nil {
		return
	}
	fmt.Println(all)
	return
}
