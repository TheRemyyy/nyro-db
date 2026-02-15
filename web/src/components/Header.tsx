import { useState } from 'react';
import { NavLink } from 'react-router-dom';
import { Menu, X, Github } from 'lucide-react';

export default function Header() {
    const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

    const LINKS = [
        { title: 'Home', path: '/' },
        { title: 'Documentation', path: '/docs' },
        { title: 'Changelog', path: '/changelog' },
    ];

    return (
        <header className="h-16 border-b border-border bg-background/80 backdrop-blur fixed top-0 left-0 right-0 z-50">
            <div className="max-w-7xl mx-auto px-4 h-full flex items-center justify-between">
                {/* Logo - Text Only */}
                <NavLink to="/" className="font-bold text-xl text-white tracking-tight hover:text-orange-500 transition-colors">
                    NyroDB
                </NavLink>

                {/* Desktop Nav */}
                <nav className="hidden md:flex items-center gap-6">
                    {LINKS.map(link => (
                        <NavLink
                            key={link.path}
                            to={link.path}
                            className={({ isActive }) => `text-sm font-medium transition-colors ${isActive ? 'text-orange-500' : 'text-zinc-400 hover:text-orange-500'}`}
                        >
                            {link.title}
                        </NavLink>
                    ))}

                    {/* Divider & Version */}
                    <div className="flex items-center gap-6 border-l border-white/10 pl-6 ml-2">
                        <div className="text-xs font-mono text-zinc-500">v1.0.0</div>

                        {/* GitHub Icon - Far Right */}
                        <a
                            href="https://github.com/TheRemyyy/nyro-db"
                            target="_blank"
                            rel="noreferrer"
                            className="text-zinc-400 hover:text-orange-500 transition-colors flex items-center gap-2"
                            title="View on GitHub"
                        >
                            <Github size={20} />
                        </a>
                    </div>
                </nav>

                {/* Mobile Toggle */}
                <button
                    className="md:hidden p-2 text-zinc-400"
                    onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
                >
                    {mobileMenuOpen ? <X /> : <Menu />}
                </button>
            </div>

            {/* Mobile Menu */}
            {mobileMenuOpen && (
                <div className="md:hidden absolute top-16 left-0 right-0 bg-background border-b border-border p-4 space-y-4 shadow-2xl">
                    {LINKS.map(link => (
                        <NavLink
                            key={link.path}
                            to={link.path}
                            onClick={() => setMobileMenuOpen(false)}
                            className={({ isActive }) => `block text-base font-medium ${isActive ? 'text-orange-500' : 'text-zinc-400 hover:text-orange-500'}`}
                        >
                            {link.title}
                        </NavLink>
                    ))}
                    <hr className="border-white/10" />
                    <a
                        href="https://github.com/TheRemyyy/nyro-db"
                        className="block text-base font-medium text-zinc-400 hover:text-orange-500 flex items-center gap-2"
                    >
                        <Github size={16} /> GitHub
                    </a>
                </div>
            )}
        </header>
    );
}

