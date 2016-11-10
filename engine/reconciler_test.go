// Copyright 2014 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"reflect"
	"testing"

	"github.com/coreos/fleet/job"
	"github.com/coreos/fleet/machine"
	"github.com/coreos/fleet/unit"
)

func newUF(t *testing.T, contents string) unit.UnitFile {
	uf, err := unit.NewUnitFile(contents)
	if err != nil {
		t.Fatalf("error creating new unit file from %v: %v", contents, err)
	}
	return *uf
}

func TestCalculateClusterTasks(t *testing.T) {
	jsInactive := job.JobStateInactive
	jsLaunched := job.JobStateLaunched

	tests := []struct {
		clust *clusterState
		tasks []*task
	}{
		// no work to do
		{
			clust: newClusterState([]job.Unit{}, []job.ScheduledUnit{}, []machine.MachineState{}),
			tasks: []*task{},
		},

		// do nothing if Job is shcheduled and target machine exists
		{
			clust: newClusterState(
				[]job.Unit{
					job.Unit{
						Name:        "foo.service",
						TargetState: job.JobStateLaunched,
					},
				},
				[]job.ScheduledUnit{
					job.ScheduledUnit{
						Name:            "foo.service",
						State:           &jsLaunched,
						TargetMachineID: "XXX",
					},
				},
				[]machine.MachineState{
					machine.MachineState{ID: "XXX"},
				},
			),
			tasks: []*task{},
		},

		// reschedule if Job's target machine is gone
		{
			clust: newClusterState(
				[]job.Unit{
					job.Unit{
						Name:        "foo.service",
						TargetState: job.JobStateLaunched,
					},
				},
				[]job.ScheduledUnit{
					job.ScheduledUnit{
						Name:            "foo.service",
						State:           &jsLaunched,
						TargetMachineID: "ZZZ",
					},
				},
				[]machine.MachineState{
					machine.MachineState{ID: "XXX"},
				},
			),
			tasks: []*task{
				&task{
					Type:      taskTypeUnscheduleUnit,
					Reason:    "target Machine(ZZZ) went away",
					JobName:   "foo.service",
					MachineID: "ZZZ",
				},
				&task{
					Type:      taskTypeAttemptScheduleUnit,
					Reason:    "target state launched and unit not scheduled",
					JobName:   "foo.service",
					MachineID: "XXX",
				},
			},
		},

		// reschedule if unbalanced
		{
			clust: newClusterState(
				[]job.Unit{
					job.Unit{
						Name:        "foo.service",
						TargetState: job.JobStateLaunched,
					},
					job.Unit{
						Name:        "bar.service",
						TargetState: job.JobStateLaunched,
					},
					job.Unit{
						Name:        "foobar.service",
						TargetState: job.JobStateLaunched,
						Unit: newUF(t, "[X-Fleet]\nCanBalance=true"),
					},
				},
				[]job.ScheduledUnit{
					job.ScheduledUnit{
						Name:            "foo.service",
						State:           &jsLaunched,
						TargetMachineID: "X1",
					},
					job.ScheduledUnit{
						Name:            "bar.service",
						State:           &jsLaunched,
						TargetMachineID: "X1",
					},
					job.ScheduledUnit{
						Name:            "foobar.service",
						State:           &jsLaunched,
						TargetMachineID: "X1",
					},
				},
				[]machine.MachineState{
					machine.MachineState{ID: "X1"},
					machine.MachineState{ID: "X2"},
					machine.MachineState{ID: "X3"},
				},
			),
			tasks: []*task{
				&task{
					Type:      taskTypeUnscheduleUnit,
					Reason:    "target Machine(X1) is unbalanced",
					JobName:   "foobar.service",
					MachineID: "X1",
				},
				&task{
					Type:      taskTypeAttemptScheduleUnit,
					Reason:    "target state launched and unit not scheduled",
					JobName:   "foobar.service",
					MachineID: "X2",
				},
			},
		},

		// do nothing is unbalance is not enough
		{
			clust: newClusterState(
				[]job.Unit{
					job.Unit{
						Name:        "foo.service",
						TargetState: job.JobStateLaunched,
					},
					job.Unit{
						Name:        "foobar.service",
						TargetState: job.JobStateLaunched,
						Unit: newUF(t, "[X-Fleet]\nCanBalance=true"),
					},
				},
				[]job.ScheduledUnit{
					job.ScheduledUnit{
						Name:            "foo.service",
						State:           &jsLaunched,
						TargetMachineID: "X1",
					},
					job.ScheduledUnit{
						Name:            "foobar.service",
						State:           &jsLaunched,
						TargetMachineID: "X1",
					},
				},
				[]machine.MachineState{
					machine.MachineState{ID: "X1"},
					machine.MachineState{ID: "X2"},
					machine.MachineState{ID: "X3"},
				},
			),
			tasks: []*task{
			},
		},

		// do nothing if balanced
		{
			clust: newClusterState(
				[]job.Unit{
					job.Unit{
						Name:        "foo.service",
						TargetState: job.JobStateLaunched,
					},
					job.Unit{
						Name:        "bar.service",
						TargetState: job.JobStateLaunched,
					},
					job.Unit{
						Name:        "bar1.service",
						TargetState: job.JobStateLaunched,
					},
					job.Unit{
						Name:        "foobar.service",
						TargetState: job.JobStateLaunched,
						Unit: newUF(t, "[X-Fleet]\nCanBalance=true"),
					},
				},
				[]job.ScheduledUnit{
					job.ScheduledUnit{
						Name:            "foo.service",
						State:           &jsLaunched,
						TargetMachineID: "X1",
					},
					job.ScheduledUnit{
						Name:            "bar.service",
						State:           &jsLaunched,
						TargetMachineID: "X1",
					},
					job.ScheduledUnit{
						Name:            "bar1.service",
						State:           &jsLaunched,
						TargetMachineID: "X1",
					},
					job.ScheduledUnit{
						Name:            "foobar.service",
						State:           &jsLaunched,
						TargetMachineID: "X3",
					},
				},
				[]machine.MachineState{
					machine.MachineState{ID: "X1"},
					machine.MachineState{ID: "X2"},
					machine.MachineState{ID: "X3"},
				},
			),
			tasks: []*task{
			},
		},

		// unschedule if Job's target state inactive and is scheduled
		{
			clust: newClusterState(
				[]job.Unit{
					job.Unit{
						Name:        "foo.service",
						TargetState: job.JobStateInactive,
					},
				},
				[]job.ScheduledUnit{
					job.ScheduledUnit{
						Name:            "foo.service",
						State:           &jsLaunched,
						TargetMachineID: "XXX",
					},
				},
				[]machine.MachineState{
					machine.MachineState{ID: "XXX"},
				},
			),
			tasks: []*task{
				&task{
					Type:      taskTypeUnscheduleUnit,
					Reason:    "target state inactive",
					JobName:   "foo.service",
					MachineID: "XXX",
				},
			},
		},

		// attempt to schedule a Job if a machine exists
		{
			clust: newClusterState(
				[]job.Unit{
					job.Unit{
						Name:        "foo.service",
						TargetState: job.JobStateLaunched,
					},
				},
				[]job.ScheduledUnit{
					job.ScheduledUnit{
						Name:            "foo.service",
						State:           &jsInactive,
						TargetMachineID: "",
					},
				},
				[]machine.MachineState{
					machine.MachineState{ID: "XXX"},
				},
			),
			tasks: []*task{
				&task{
					Type:      taskTypeAttemptScheduleUnit,
					Reason:    "target state launched and unit not scheduled",
					JobName:   "foo.service",
					MachineID: "XXX",
				},
			},
		},
	}

	for i, tt := range tests {
		r := NewReconciler()
		tasks := make([]*task, 0)
		for tsk := range r.calculateClusterTasks(tt.clust, make(chan struct{})) {
			tasks = append(tasks, tsk)
		}

		if !reflect.DeepEqual(tt.tasks, tasks) {
			t.Errorf("case %d: task mismatch\nexpected %v\n got %v", i, tt.tasks, tasks)
		}
	}
}
