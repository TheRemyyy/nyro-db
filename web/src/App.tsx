import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { Suspense, lazy } from 'react';
import { Analytics } from '@vercel/analytics/react';
import Header from './components/Header';
import Home from './pages/Home';
import Footer from './components/Footer';
import ScrollToTop from './components/ScrollToTop';

const Docs = lazy(() => import('./pages/Docs'));

function App() {
  return (
    <Router>
      <ScrollToTop />
      <div className="min-h-screen bg-background text-zinc-100 font-sans selection:bg-orange-500/30 flex flex-col">
        <Header />
        <Suspense fallback={<div className="flex-1 bg-background" />}>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/docs/*" element={<Docs />} />
          </Routes>
        </Suspense>
        <Footer />
      </div>
      <Analytics />
    </Router>
  )
}

export default App
