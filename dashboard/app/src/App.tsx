import { Routes, Route } from 'react-router-dom'
import { AppLayout } from '@/components/layout/AppLayout'
import PipelineMonitor from '@/pages/PipelineMonitor'
import ErrorIntelligence from '@/pages/ErrorIntelligence'
import AdminGovernance from '@/pages/AdminGovernance'
import FlowExplorer from '@/pages/FlowExplorer'
import SourceManager from '@/pages/SourceManager'

function App() {
  return (
    <AppLayout>
      <Routes>
        <Route path="/" element={<PipelineMonitor />} />
        <Route path="/errors" element={<ErrorIntelligence />} />
        <Route path="/admin" element={<AdminGovernance />} />
        <Route path="/flow" element={<FlowExplorer />} />
        <Route path="/sources" element={<SourceManager />} />
      </Routes>
    </AppLayout>
  )
}

export default App
