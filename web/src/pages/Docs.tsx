import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { marked } from 'marked';
import hljs from 'highlight.js';
import 'highlight.js/styles/github-dark.css';
import { Sidebar, DOCS_NAV } from '../components/Sidebar';
import { Menu, X, ChevronRight, ChevronLeft } from 'lucide-react';

export default function Docs() {
    const { "*": splat } = useParams();
    const navigate = useNavigate();
    const [content, setContent] = useState<string>('');
    const [isLoading, setIsLoading] = useState(true);
    const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

    // Normalize path
    const currentPath = splat ? `/docs/${splat}` : '/docs/overview';

    // Flatten nav for prev/next logic
    const flatNav = DOCS_NAV.flatMap(s => s.items);
    const currentIndex = flatNav.findIndex(i => i.path === currentPath);
    const prevPage = currentIndex > 0 ? flatNav[currentIndex - 1] : null;
    const nextPage = currentIndex < flatNav.length - 1 ? flatNav[currentIndex + 1] : null;

    useEffect(() => {
        const fetchDoc = async () => {
            setIsLoading(true);
            try {
                // Determine file path: /docs/overview -> /docs/overview.md
                // /docs/getting_started/installation -> /docs/getting_started/installation.md
                let filePath = currentPath.replace('/docs', '');
                if (filePath === '' || filePath === '/') filePath = '/overview';

                const res = await fetch(`/docs${filePath}.md`);
                if (!res.ok) throw new Error('Doc not found');
                const text = await res.text();

                // Configure marked options
                marked.use({
                    gfm: true,
                    breaks: true, // Render newlines as <br>
                });

                setContent(marked.parse(text) as string);

                // Ensure scroll happens after render and paint
                setTimeout(() => {
                    window.scrollTo({ top: 0, left: 0, behavior: 'instant' });
                }, 10);
            } catch (err) {
                console.error(err);
                if (currentPath !== '/docs/overview') {
                    // Fallback or 404
                    setContent('# 404 Not Found\nRequest document could not be loaded.');
                }
            } finally {
                setIsLoading(false);
                setMobileMenuOpen(false);
            }
        };

        fetchDoc();
    }, [currentPath]);

    // Apply syntax highlighting after content update
    useEffect(() => {
        if (content) {
            hljs.highlightAll();
        }
    }, [content]);

    return (
        <div className="flex flex-col md:flex-row min-h-screen bg-background text-zinc-300 pt-16">
            {/* Mobile Header */}
            <div className="md:hidden flex items-center justify-between p-4 border-b border-border bg-background/80 backdrop-blur sticky top-16 z-30">
                <span className="font-semibold text-lg text-white">Documentation</span>
                <button onClick={() => setMobileMenuOpen(!mobileMenuOpen)} className="p-2 text-zinc-400 hover:text-white">
                    {mobileMenuOpen ? <X /> : <Menu />}
                </button>
            </div>

            {/* Sidebar */}
            <div className={`fixed inset-0 top-16 z-40 md:static md:z-auto bg-background md:bg-transparent transform transition-transform duration-300 md:translate-x-0 ${mobileMenuOpen ? 'translate-x-0' : '-translate-x-full'}`}>
                <Sidebar onClose={() => setMobileMenuOpen(false)} />
            </div>

            {/* Main Content */}
            <main className="flex-1 min-w-0 py-8 px-4 md:px-12 lg:px-16 max-w-5xl mx-auto w-full">
                {isLoading ? (
                    <div className="animate-pulse space-y-4 max-w-2xl mt-8">
                        <div className="h-8 bg-surface rounded w-3/4"></div>
                        <div className="h-4 bg-surface rounded w-full"></div>
                        <div className="h-4 bg-surface rounded w-5/6"></div>
                        <div className="h-32 bg-surface rounded w-full mt-8"></div>
                    </div>
                ) : (
                    <div className="prose prose-invert prose-zinc max-w-none prose-headings:text-white prose-a:text-primary prose-code:bg-surface prose-code:text-primary prose-code:px-1 prose-code:rounded prose-pre:bg-surface prose-pre:border prose-pre:border-border">
                        <div dangerouslySetInnerHTML={{ __html: content }} />
                    </div>
                )}

                {/* Navigation Footer */}
                <div className="mt-16 pt-8 border-t border-border flex flex-col sm:flex-row gap-4 justify-between">
                    {prevPage ? (
                        <button
                            onClick={() => navigate(prevPage.path)}
                            className="flex items-center gap-2 group text-sm text-zinc-400 hover:text-primary transition-colors text-left"
                        >
                            <div className="p-2 rounded-full bg-surface border border-border group-hover:border-primary/50 transition-colors">
                                <ChevronLeft size={16} />
                            </div>
                            <div>
                                <div className="text-xs text-zinc-500 mb-0.5">Previous</div>
                                <div className="font-medium text-white group-hover:text-primary">{prevPage.title}</div>
                            </div>
                        </button>
                    ) : <div></div>}

                    {nextPage && (
                        <button
                            onClick={() => navigate(nextPage.path)}
                            className="flex items-center gap-2 group text-sm text-zinc-400 hover:text-primary transition-colors text-right flex-row-reverse"
                        >
                            <div className="p-2 rounded-full bg-surface border border-border group-hover:border-primary/50 transition-colors">
                                <ChevronRight size={16} />
                            </div>
                            <div>
                                <div className="text-xs text-zinc-500 mb-0.5">Next</div>
                                <div className="font-medium text-white group-hover:text-primary">{nextPage.title}</div>
                            </div>
                        </button>
                    )}
                </div>
            </main>
        </div>
    );
}
