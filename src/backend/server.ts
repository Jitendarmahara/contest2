import express from "express"
import { podPool } from "../k8s/pod-pool"
import { orchestrator } from "./orchestrator"
import { get } from "node:http"
import { getWorkflow } from "./workflow-store"

const app = express()
app.use(express.json())

// POST /workflow
// Students implement: store workflow, find ready steps, enqueue them
app.post("/workflow", async (req, res) => {
  const workflow = req.body

  // TODO: students implement this in orchestrator.ts
  // Call orchestrator.submitWorkflow(workflow)
  await orchestrator.submitWorkflow(workflow)
  res.status(201).json({ workflowId: workflow.workflowId })
})

// GET /workflow/:id
// Students implement: read from workflow-store and return current state
app.get("/workflow/:id", (req, res) => {
  const workflowId = req.params.id

  // TODO: students implement this
  const wf = getWorkflow(workflowId)
  if(!wf){
    return res.status(400).json({error:"workflow not found"})
  }
  const steps = wf.steps.map((x)=>{
    const state = wf.stepState[x.id];
    return{
      id: x.id,
      status: state.status,
      podId: state.podId ?? null,
      ...((state.exitCode !== undefined) &&{exitCOde: state.exitCode}),
      ...((state.stdout !== undefined) && {stdout: state.stdout})
    }
  })

  return res.json({
    workflowId,
    status: wf.status,
    steps
  })
})

// GET /pods  — already implemented, useful for debugging
app.get("/pods", async (_req, res) => {
  const status = podPool.getPoolStatus()
  res.json(status)
})

export { app }
