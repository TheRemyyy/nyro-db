import { Github, Twitter, Heart, MessageCircle, BookOpen, Server } from 'lucide-react';
import { NavLink } from 'react-router-dom';

export default function Footer() {
    return (
        <footer className="border-t border-border bg-background pt-16 pb-8 mt-auto">
            <div className="max-w-7xl mx-auto px-4 grid grid-cols-1 md:grid-cols-4 gap-12 mb-16">

                {/* Brand Column */}
                <div className="md:col-span-2 flex flex-col items-start gap-4">
                    <div className="flex items-center gap-2 font-bold text-white text-xl tracking-tight">
                        NyroDB
                    </div>
                    <p className="text-zinc-500 text-sm max-w-sm leading-relaxed">
                        The next-generation database engine engineered for extreme performance and zero-copy architecture.
                    </p>
                </div>

                {/* Resources Column (Now on the left of Community) */}
                <div className="flex flex-col gap-4">
                    <h3 className="text-sm font-semibold text-white uppercase tracking-wider">Resources</h3>
                    <div className="flex flex-col gap-3">
                        <NavLink to="/docs" className="text-zinc-400 hover:text-primary transition-colors text-sm flex items-center gap-2">
                            <BookOpen size={16} /> Documentation
                        </NavLink>
                        <NavLink to="/docs/getting_started/installation" className="text-zinc-400 hover:text-primary transition-colors text-sm flex items-center gap-2">
                            <Server size={16} /> Installation
                        </NavLink>
                    </div>
                </div>

                {/* Community Column */}
                <div className="flex flex-col gap-4">
                    <h3 className="text-sm font-semibold text-white uppercase tracking-wider">Community</h3>
                    <div className="flex flex-col gap-3">
                        <a href="https://github.com/TheRemyyy/nyro-db" target="_blank" rel="noreferrer" className="text-zinc-400 hover:text-primary transition-colors text-sm flex items-center gap-2">
                            <Github size={16} /> GitHub
                        </a>
                        <span className="text-zinc-600 text-sm flex items-center gap-2 cursor-not-allowed">
                            <Twitter size={16} /> Twitter <span className="text-[10px] bg-zinc-800 text-zinc-500 px-1.5 py-0.5 rounded border border-zinc-700">Soon</span>
                        </span>
                        <span className="text-zinc-600 text-sm flex items-center gap-2 cursor-not-allowed">
                            <MessageCircle size={16} /> Discord <span className="text-[10px] bg-zinc-800 text-zinc-500 px-1.5 py-0.5 rounded border border-zinc-700">Soon</span>
                        </span>
                    </div>
                </div>
            </div>

            {/* Bottom Bar */}
            <div className="max-w-7xl mx-auto px-4 pt-8 border-t border-white/5 flex flex-col md:flex-row justify-between items-center gap-4 text-xs text-zinc-600">
                <div>
                    &copy; {new Date().getFullYear()} NyroDB Contributors. MIT License.
                </div>
                <div className="flex items-center gap-1.5">
                    <span>Built with</span>
                    <Heart size={12} className="text-red-500 fill-red-500" />
                    <span>in Rust</span>
                </div>
            </div>
        </footer>
    );
}

