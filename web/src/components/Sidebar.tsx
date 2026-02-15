import { NavLink, useLocation } from 'react-router-dom';
import { ChevronRight, Database, Server, Cpu, BookOpen, Code, Settings } from 'lucide-react';
import { motion } from 'framer-motion';

export const DOCS_NAV = [
    {
        title: 'Introduction',
        items: [
            { title: 'Overview', path: '/docs/overview', icon: BookOpen },
        ]
    },
    {
        title: 'Getting Started',
        items: [
            { title: 'Installation', path: '/docs/getting_started/installation', icon: Server },
            { title: 'Configuration', path: '/docs/getting_started/configuration', icon: Settings },
        ]
    },
    {
        title: 'API Reference',
        items: [
            { title: 'REST API', path: '/docs/api/rest', icon: Code },
            { title: 'Real-Time API', path: '/docs/api/realtime', icon: Database },
        ]
    },
    {
        title: 'Internals',
        items: [
            { title: 'Architecture', path: '/docs/internals/architecture', icon: Cpu },
        ]
    }
];

export function Sidebar({ onClose }: { onClose?: () => void }) {
    const location = useLocation();

    return (
        <aside className="w-full md:w-64 shrink-0 md:border-r border-border bg-background/50 backdrop-blur-xl h-[calc(100vh-4rem)] sticky top-16 overflow-y-auto py-6 px-4">
            <nav className="space-y-8">
                {DOCS_NAV.map((section, idx) => (
                    <div key={idx}>
                        <h3 className="text-sm font-semibold text-zinc-400 uppercase tracking-wider mb-3 px-2">
                            {section.title}
                        </h3>
                        <ul className="space-y-1">
                            {section.items.map((item) => {
                                const isActive = location.pathname === item.path || (item.path === '/docs/overview' && location.pathname === '/docs');
                                const Icon = item.icon;
                                return (
                                    <li key={item.path}>
                                        <NavLink
                                            to={item.path}
                                            onClick={onClose}
                                            className={`group flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors duration-200 outline-none focus:outline-none focus:ring-0 ${isActive
                                                ? 'bg-surface text-orange-500 border border-border shadow-sm'
                                                : 'text-zinc-400 hover:text-zinc-100 hover:bg-surface/50 border border-transparent'
                                                }`}
                                        >
                                            <Icon size={16} className={`transition-colors ${isActive ? 'text-orange-500' : 'text-zinc-500 group-hover:text-zinc-300'}`} />
                                            {item.title}
                                            {isActive && (
                                                <motion.div
                                                    layoutId="active-nav"
                                                    className="ml-auto"
                                                    initial={{ opacity: 0, x: -5 }}
                                                    animate={{ opacity: 1, x: 0 }}
                                                >
                                                    <ChevronRight size={14} className="text-orange-500" />
                                                </motion.div>
                                            )}
                                        </NavLink>
                                    </li>
                                );
                            })}
                        </ul>
                    </div>
                ))}
            </nav>
        </aside>
    );
}
