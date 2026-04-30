import { BookOpen, Code, Cpu, Database, Server, Settings } from 'lucide-react';

export const DOCS_NAV = [
    {
        title: 'Introduction',
        items: [
            { title: 'Overview', path: '/docs/overview', icon: BookOpen },
        ],
    },
    {
        title: 'Getting Started',
        items: [
            { title: 'Installation', path: '/docs/getting_started/installation', icon: Server },
            { title: 'Configuration', path: '/docs/getting_started/configuration', icon: Settings },
        ],
    },
    {
        title: 'API Reference',
        items: [
            { title: 'REST API', path: '/docs/api/rest', icon: Code },
            { title: 'Real-Time API', path: '/docs/api/realtime', icon: Database },
        ],
    },
    {
        title: 'Internals',
        items: [
            { title: 'Architecture', path: '/docs/internals/architecture', icon: Cpu },
        ],
    },
];
