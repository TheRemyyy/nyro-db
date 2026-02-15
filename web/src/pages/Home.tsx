import { NavLink } from 'react-router-dom';
import { motion } from 'framer-motion';
import { ArrowRight } from 'lucide-react';

export default function Home() {
    return (
        <div className="min-h-screen bg-background text-zinc-100 overflow-hidden font-sans">
            {/* Hero Section - Balanced Padding Top */}
            <section className="relative pt-40 pb-20 px-4 md:pt-52">
                <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-orange-900/20 via-background to-background pointer-events-none"></div>

                <div className="max-w-5xl mx-auto text-center relative z-10">
                    <motion.div
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.5 }}
                    >
                        <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-orange-500/10 border border-orange-500/20 text-orange-400 text-xs font-medium mb-6">
                            <span className="relative flex h-2 w-2">
                                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-orange-400 opacity-75"></span>
                                <span className="relative inline-flex rounded-full h-2 w-2 bg-orange-500"></span>
                            </span>
                            v1.0.0 Stable Release
                        </div>

                        <h1 className="text-4xl md:text-7xl font-bold tracking-tight mb-6 bg-clip-text text-transparent bg-gradient-to-br from-white via-zinc-200 to-zinc-500 leading-tight">
                            The Unified Storage Engine <br />
                            <span className="text-white">for Hyper-Scale Apps.</span>
                        </h1>

                        <p className="text-xl text-zinc-400 max-w-3xl mx-auto mb-10 leading-relaxed">
                            Stop splitting your data between slow databases and fragile caches. 
                            <span className="text-zinc-100 font-semibold block mt-2">NyroDB is a zero-copy, memory-mapped engine that delivers 1M+ ops/sec with persistent reliability.</span>
                        </p>

                        <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
                            <NavLink
                                to="/docs/getting_started/installation"
                                className="px-8 py-3.5 rounded-lg bg-orange-600 hover:bg-orange-700 text-white font-semibold transition-all shadow-lg shadow-orange-500/20 flex items-center gap-2"
                            >
                                Build Fast <ArrowRight size={18} />
                            </NavLink>
                            <NavLink
                                to="/docs"
                                className="px-8 py-3.5 rounded-lg bg-zinc-900 border border-zinc-800 hover:border-zinc-700 text-zinc-300 font-medium transition-all"
                            >
                                Explore Docs
                            </NavLink>
                        </div>
                    </motion.div>
                </div>
            </section>

            {/* Cache-aside: before vs after */}
            <section className="py-20 md:py-28 px-4 border-y border-white/5">
                <div className="max-w-5xl mx-auto">
                    <h2 className="text-2xl md:text-3xl font-bold text-white tracking-tight mb-12 md:mb-16 text-center">
                        Killing the cache-aside complexity
                    </h2>

                    <div className="grid md:grid-cols-2 gap-8 md:gap-12 mb-12">
                        <div className="p-6 md:p-8 rounded-lg border border-zinc-800 bg-zinc-900/30">
                            <p className="text-xs font-semibold text-zinc-500 uppercase tracking-wider mb-4">Without NyroDB</p>
                            <p className="text-zinc-400 leading-relaxed">
                                Database for durability, Redis or Memcached for speed, plus pipelines, invalidation, and hope that both stay in sync. Two systems, more moving parts.
                            </p>
                        </div>
                        <div className="p-6 md:p-8 rounded-lg border border-orange-500/20 bg-orange-500/5">
                            <p className="text-xs font-semibold text-orange-400 uppercase tracking-wider mb-4">With NyroDB</p>
                            <p className="text-zinc-300 leading-relaxed">
                                One engine: memory-mapped, zero-copy, fast as RAM and durable as disk. Single source of truth: no cache layer, no sync. Built for scale.
                            </p>
                        </div>
                    </div>

                    <div className="flex flex-wrap justify-center gap-x-8 gap-y-2 text-sm text-zinc-500">
                        <span><strong className="text-white font-semibold">1M+</strong> ops/sec</span>
                        <span><strong className="text-white font-semibold">&lt;1μs</strong> latency</span>
                        <span><strong className="text-white font-semibold">0</strong> copy overhead</span>
                        <span><strong className="text-white font-semibold">∞</strong> scale</span>
                    </div>
                </div>
            </section>

            {/* Core Architecture - Compact */}
            <section className="py-24 px-4">
                <div className="max-w-3xl mx-auto text-center">
                    <h2 className="text-3xl font-bold mb-3 text-white tracking-tight">Core Architecture</h2>
                    <p className="text-zinc-400 leading-relaxed mb-8">
                        High throughput, ACID persistence, O(1) indexing, real-time WebSockets, zero-copy Rust, and multi-tenant auth, all in one engine.
                    </p>
                    <div className="flex flex-wrap justify-center gap-2">
                        {['Throughput', 'ACID', 'O(1) indexing', 'WebSocket', 'Zero-copy', 'Multi-tenant'].map((label) => (
                            <span key={label} className="px-3 py-1.5 text-xs font-medium text-zinc-400 bg-zinc-800/60 border border-zinc-700/50 rounded">
                                {label}
                            </span>
                        ))}
                    </div>
                </div>
            </section>

            {/* Code Demo */}
            <section className="py-20 px-4 bg-zinc-950 border-t border-white/5">
                <div className="max-w-5xl mx-auto flex flex-col md:flex-row items-center gap-12">
                    <div className="flex-1 space-y-6">
                        <h2 className="text-3xl font-bold">Simple, Declarative API</h2>
                        <p className="text-zinc-400">
                            NyroDB abstracts the complexity of mmap and thread-safety behind a clean REST and WebSocket interface.
                        </p>
                        <div className="space-y-4">
                            <CheckItem text="RESTful endpoints for instant CRUD" />
                            <CheckItem text="Schema-less JSON native storage" />
                            <CheckItem text="Instant pub/sub over WebSockets" />
                            <CheckItem text="Metrics endpoint for real-time monitoring" />
                        </div>
                    </div>

                    <div className="flex-1 w-full relative">
                        <div className="absolute inset-0 bg-orange-500/10 blur-3xl rounded-full"></div>
                        <div className="relative bg-[#09090b] border border-zinc-800 rounded-xl p-6 shadow-2xl font-mono text-sm overflow-hidden">
                            <div className="flex gap-2 mb-4">
                                <div className="w-3 h-3 rounded-full bg-red-500/20 border border-red-500/50"></div>
                                <div className="w-3 h-3 rounded-full bg-yellow-500/20 border border-yellow-500/50"></div>
                                <div className="w-3 h-3 rounded-full bg-green-500/20 border border-green-500/50"></div>
                            </div>
                            <div className="text-zinc-400">
                                <span className="text-purple-400">POST</span> /insert/metrics <span className="text-zinc-600">HTTP/1.1</span><br />
                                <span className="text-blue-400">Content-Type:</span> application/json<br />
                                <br />
                                <span className="text-zinc-300">{`{`}</span><br />
                                &nbsp;&nbsp;<span className="text-green-400">"service"</span>: <span className="text-yellow-300">"auth-node-1"</span>,<br />
                                &nbsp;&nbsp;<span className="text-green-400">"tps"</span>: <span className="text-orange-400">1250000</span>,<br />
                                &nbsp;&nbsp;<span className="text-green-400">"status"</span>: <span className="text-yellow-300">"healthy"</span><br />
                                <span className="text-zinc-300">{`}`}</span>
                            </div>
                        </div>
                    </div>
                </div>
            </section>
        </div>
    );
}

function CheckItem({ text }: { text: string }) {
    return (
        <div className="flex items-center gap-3">
            <div className="w-5 h-5 rounded-full bg-orange-500/10 border border-orange-500/20 flex items-center justify-center">
                <div className="w-1.5 h-1.5 rounded-full bg-orange-500"></div>
            </div>
            <span className="text-zinc-300">{text}</span>
        </div>
    );
}
