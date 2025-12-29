import { NavLink } from 'react-router-dom';
import { motion } from 'framer-motion';
import { Zap, Database, Globe, ArrowRight, ShieldCheck, Cpu, Layers } from 'lucide-react';

export default function Home() {
    return (
        <div className="min-h-screen bg-background text-zinc-100 overflow-hidden font-sans">
            {/* Hero Section */}
            <section className="relative pt-32 pb-20 px-4">
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

            {/* About / Vision Section */}
            <section className="py-24 px-4 bg-zinc-950/50 border-y border-white/5">
                <div className="max-w-5xl mx-auto">
                    <div className="grid md:grid-cols-2 gap-16 items-center">
                        <div>
                            <h2 className="text-3xl font-bold mb-6 text-white tracking-tight">Killing the Cache-Aside Complexity.</h2>
                            <div className="space-y-4 text-zinc-400 leading-relaxed text-lg">
                                <p>
                                    Modern infrastructure is broken. We store data in "slow" databases and then build massive, complex sync layers to keep "fast" caches (Redis/Memcached) updated.
                                </p>
                                <p>
                                    <span className="text-orange-400 font-medium italic">NyroDB was born to kill this pattern.</span> By combining memory-mapped files with zero-copy serialization, we've created a system that is as fast as your RAM but as reliable as your NVMe.
                                </p>
                                <p>
                                    Whether you're building high-frequency trading platforms or massive real-time games, NyroDB gives you a single source of truth that never lags.
                                </p>
                            </div>
                        </div>
                        <div className="grid grid-cols-2 gap-4">
                            <div className="p-6 rounded-xl border border-zinc-800 bg-black flex flex-col items-center text-center">
                                <span className="text-3xl font-bold text-white mb-1">1M+</span>
                                <span className="text-xs text-zinc-500 uppercase tracking-widest">Ops/Sec</span>
                            </div>
                            <div className="p-6 rounded-xl border border-zinc-800 bg-black flex flex-col items-center text-center">
                                <span className="text-3xl font-bold text-white mb-1">&lt;1μs</span>
                                <span className="text-xs text-zinc-500 uppercase tracking-widest">Latency</span>
                            </div>
                            <div className="p-6 rounded-xl border border-zinc-800 bg-black flex flex-col items-center text-center">
                                <span className="text-3xl font-bold text-white mb-1">0</span>
                                <span className="text-xs text-zinc-500 uppercase tracking-widest">Copy Overh.</span>
                            </div>
                            <div className="p-6 rounded-xl border border-zinc-800 bg-black flex flex-col items-center text-center">
                                <span className="text-3xl font-bold text-white mb-1">∞</span>
                                <span className="text-xs text-zinc-500 uppercase tracking-widest">Scalability</span>
                            </div>
                        </div>
                    </div>
                </div>
            </section>

            {/* Features Grid */}
            <section className="py-24 px-4">
                <div className="text-center mb-16">
                    <h2 className="text-3xl font-bold mb-4">Core Architecture</h2>
                    <p className="text-zinc-500">Built for the limits of modern hardware.</p>
                </div>
                <div className="max-w-6xl mx-auto grid md:grid-cols-3 gap-8">
                    <FeatureCard
                        icon={Zap}
                        title="Extreme Throughput"
                        desc="Utilizes asynchronous batching and wait-free data structures to saturate your I/O subsystem."
                    />
                    <FeatureCard
                        icon={Globe}
                        title="Native WebSocket Layer"
                        desc="A built-in pub/sub engine allows clients to subscribe to specific JSON paths and get updates in real-time."
                    />
                    <FeatureCard
                        icon={Database}
                        title="O(1) Secondary Indexing"
                        desc="NyroDB automatically indexes your JSON fields without schema definitions or heavy migration scripts."
                    />
                    <FeatureCard
                        icon={ShieldCheck}
                        title="ACID Persistence"
                        desc="Atomic, Consistent, Isolated, and Durable. Your data is safe even during power failure via WAL recovery."
                    />
                    <FeatureCard
                        icon={Cpu}
                        title="Rust & Zero-Copy"
                        desc="Data is accessed directly via memory pointers. No serialization overhead, no GC, no compromises."
                    />
                    <FeatureCard
                        icon={Layers}
                        title="Multi-Tenant Auth"
                        desc="Strict API-key validation and namespace isolation for secure multi-application environments."
                    />
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

function FeatureCard({ icon: Icon, title, desc }: { icon: any, title: string, desc: string }) { 
    return (
        <div className="p-8 rounded-xl bg-zinc-900/40 border border-zinc-800 hover:border-orange-500/50 transition-all group">
            <div className="w-12 h-12 rounded-lg bg-zinc-900 flex items-center justify-center mb-6 group-hover:bg-orange-500/10 transition-colors border border-zinc-800 group-hover:border-orange-500/20">
                <Icon className="text-zinc-500 group-hover:text-orange-400 transition-colors" size={24} />
            </div>
            <h3 className="text-xl font-bold text-white mb-3 tracking-tight">{title}</h3>
            <p className="text-zinc-400 leading-relaxed">{desc}</p>
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


