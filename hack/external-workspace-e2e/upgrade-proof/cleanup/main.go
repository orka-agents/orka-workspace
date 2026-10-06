// Package main deletes only the exact synthetic object lifetimes recorded by
// the local upgrade proof. It has no dependency on Orka runtime implementation.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

type identity struct {
	Group, Version, Resource, Namespace, Name, UID string
}

func main() {
	input := flag.String("input", "", "JSON list of exact synthetic object lifetimes")
	flag.Parse()
	if err := run(*input); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(input string) error {
	path := os.Getenv("KUBECONFIG")
	if input == "" || path == "" {
		return fmt.Errorf("input and kindctl-scoped KUBECONFIG are required")
	}
	raw, err := os.ReadFile(input)
	if err != nil {
		return err
	}
	var objects []identity
	if err := json.Unmarshal(raw, &objects); err != nil {
		return err
	}
	config, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return err
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, object := range objects {
		if object.Version == "" || object.Resource == "" || object.Name == "" || object.UID == "" {
			return fmt.Errorf("incomplete deletion identity")
		}
		resource := client.Resource(schema.GroupVersionResource{Group: object.Group, Version: object.Version, Resource: object.Resource}).Namespace(object.Namespace)
		current, err := resource.Get(ctx, object.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if string(current.GetUID()) != object.UID {
			return fmt.Errorf("refusing replacement %s %s/%s", object.Resource, object.Namespace, object.Name)
		}
		uid, version := types.UID(object.UID), current.GetResourceVersion()
		foreground := metav1.DeletePropagationForeground
		if err := resource.Delete(ctx, object.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}, PropagationPolicy: &foreground}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		for {
			current, err := resource.Get(ctx, object.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				break
			}
			if err != nil {
				return err
			}
			if string(current.GetUID()) != object.UID {
				return fmt.Errorf("replacement appeared while deleting %s %s/%s", object.Resource, object.Namespace, object.Name)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return nil
}
