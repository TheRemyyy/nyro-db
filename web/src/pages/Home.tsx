import { NavLink } from 'react-router-dom';
import { motion } from 'framer-motion';
import { Zap, Database, Globe, ArrowRight, Server, ShieldCheck } from 'lucide-react';

export default function Home() {
    return (
        <div className="min-h-screen bg-background text-zinc-100 overflow-hidden">
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
                            v1.0.0 Now Available
                        </div>

                        <h1 className="text-4xl md:text-7xl font-bold tracking-tight mb-6 bg-clip-text text-transparent bg-gradient-to-br from-white via-zinc-200 to-zinc-500">
                            The Fastest Real-Time DB <br />
                            <span className="text-white">in the Known Universe.</span>
                        </h1>

                        <p className="text-xl text-zinc-400 max-w-2xl mx-auto mb-10 leading-relaxed">
                            Built with Rust. Zero-copy architecture. <span className="text-zinc-100 font-semibold">1M+ ops/sec</span>.
                            NyroDB bridges the gap between in-memory caches and persistent storage.
                        </p>

                        <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
                            <NavLink
                                to="/docs/getting_started/installation"
                                className="px-8 py-3.5 rounded-lg bg-primary hover:bg-orange-700 text-white font-semibold transition-all shadow-lg shadow-orange-500/20 flex items-center gap-2"
                            >
                                Get Started <ArrowRight size={18} />
                            </NavLink>
                            <NavLink
                                to="/docs"
                                className="px-8 py-3.5 rounded-lg bg-surface border border-border hover:border-zinc-600 text-zinc-300 font-medium transition-all"
                            >
                                Read Documentation
                            </NavLink>
                        </div>
                    </motion.div>
                </div>
            </section>

            {/* Features Grid */}
            <section className="py-24 px-4 border-t border-white/5">
                <div className="max-w-6xl mx-auto grid md:grid-cols-3 gap-8">
                    <FeatureCard
                        icon={Zap}
                        title="Extreme Performance"
                        desc="Capable of over 1,000,000 operations per second with sub-microsecond latency using mmap and atomic batching."
                    />
                    <FeatureCard
                        icon={Globe}
                        title="Native Real-Time"
                        desc="Built-in WebSocket server pushes updates instantly. No external message queues (Redis/Kafka) required."
                    />
                    <FeatureCard
                        icon={Database}
                        title="Universal Indexing"
                        desc="O(1) secondary lookups on any JSON field. Query by metadata instantly without schema migrations."
                    />
                    <FeatureCard
                        icon={ShieldCheck}
                        title="Production Ready"
                        desc="ACID-compliant persistence with WAL recovery. Secure API Key authentication built-in."
                    />
                    <FeatureCard
                        icon={Server}
                        title="Rust Powered"
                        desc="Memory safe, thread safe, and compiled to a single binary. Zero garbage collection pauses."
                    />
                    <FeatureCard
                        icon={ArrowRight}
                        title="Zero-Copy Storage"
                        desc="Data maps directly from disk to memory, bypassing user-space copy overhead entirely."
                    />
                </div>
            </section>

            {/* Code Demo */}
            <section className="py-20 px-4 bg-surface/30">
                <div className="max-w-5xl mx-auto flex flex-col md:flex-row items-center gap-12">
                    <div className="flex-1 space-y-6">
                        <h2 className="text-3xl font-bold">Simple yet Powerful API</h2>
                        <p className="text-zinc-400">
                            Interact via standard HTTP or WebSocket. Client libraries available for all major languages.
                        </p>
                        <div className="space-y-4">
                            <CheckItem text="RESTful endpoints for CRUD" />
                            <CheckItem text="WebSocket Pub/Sub" />
                            <CheckItem text="JSON Native Storage" />
                        </div>
                    </div>

                    <div className="flex-1 w-full relative">
                        <div className="absolute inset-0 bg-primary/20 blur-3xl rounded-full"></div>
                        <div className="relative bg-[#0d0d10] border border-border rounded-xl p-6 shadow-2xl font-mono text-sm overflow-hidden">
                            <div className="flex gap-2 mb-4">
                                <div className="w-3 h-3 rounded-full bg-red-500/20 border border-red-500/50"></div>
                                <div className="w-3 h-3 rounded-full bg-yellow-500/20 border border-yellow-500/50"></div>
                                <div className="w-3 h-3 rounded-full bg-green-500/20 border border-green-500/50"></div>
                            </div>
                            <div className="text-zinc-400">
                                <span className="text-purple-400">POST</span> /insert/user <span className="text-zinc-600">HTTP/1.1</span><br />
                                <span className="text-blue-400">Content-Type:</span> application/json<br />
                                <span className="text-blue-400">x-api-key:</span> secret_key<br />
                                <br />
                                <span className="text-zinc-300">{`{`}</span><br />
                                &nbsp;&nbsp;<span className="text-green-400">"id"</span>: <span className="text-orange-400">1024</span>,<br />
                                &nbsp;&nbsp;<span className="text-green-400">"email"</span>: <span className="text-yellow-300">"nyro@db.io"</span>,<br />
                                &nbsp;&nbsp;<span className="text-green-400">"role"</span>: <span className="text-yellow-300">"admin"</span><br />
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
        <div className="p-6 rounded-xl bg-surface/40 border border-white/5 hover:border-primary/50 transition-colors group">
            <div className="w-12 h-12 rounded-lg bg-surface flex items-center justify-center mb-4 group-hover:bg-primary/20 transition-colors">
                <Icon className="text-zinc-400 group-hover:text-primary transition-colors" size={24} />
            </div>
            <h3 className="text-lg font-bold text-white mb-2">{title}</h3>
            <p className="text-sm text-zinc-400 leading-relaxed">{desc}</p>
        </div>
    );
}

function CheckItem({ text }: { text: string }) {
    return (
        <div className="flex items-center gap-3">
            <div className="w-5 h-5 rounded-full bg-green-500/20 flex items-center justify-center">
                <div className="w-1.5 h-1.5 rounded-full bg-green-500"></div>
            </div>
            <span className="text-zinc-300">{text}</span>
        </div>
    );
}
