package main

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/divinerapier/scheduled/go/scheduled/api/v1alpha1"
)

func MustT[T any](v T, err error) T {
	Must(err)
	return v
}

func Must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	scheme := runtime.NewScheme()
	v1alpha1.AddToScheme(scheme)

	config := ctrl.GetConfigOrDie()
	cache := MustT(cache.New(config, cache.Options{Scheme: scheme}))

	c := MustT(client.New(config, client.Options{
		Scheme: scheme,
		Cache: &client.CacheOptions{
			Reader: cache,
		},
	}))

	Must(c.Create(context.Background(), &v1alpha1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "go-cronjob",
			Namespace: "default",
		},
		Spec: v1alpha1.CronJobSpec{
			Schedule: &v1alpha1.ScheduleRule{
				ConcurrencyPolicy: v1alpha1.ForbidConcurrent,
				Schedule: []v1alpha1.ScheduleType{{
					Interval: &v1alpha1.Interval{
						Seconds: 30,
					},
				}},
			},
			Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers: []corev1.Container{{
							Name:  "test-container",
							Image: "ubuntu:22.04",
							Command: []string{
								"/bin/bash",
								"-c",
								"echo \"starting ...\" && sleep 10 && echo \"done\"",
							},
						}},
					},
				},
			},
		},
	}))

}
