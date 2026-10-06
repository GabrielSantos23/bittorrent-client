import {
  useEffect,
  useState,
  type AnchorHTMLAttributes,
  type Ref,
} from "react";

// Minimal stand-ins for next/link and next/navigation on Vite (no router).
export function Link({
  href,
  children,
  ref,
  ...props
}: AnchorHTMLAttributes<HTMLAnchorElement> & {
  href: string;
  ref?: Ref<HTMLAnchorElement>;
}) {
  return (
    <a ref={ref} href={href} {...props}>
      {children}
    </a>
  );
}

export function usePathname() {
  const [pathname, setPathname] = useState(() => window.location.pathname);
  useEffect(() => {
    const update = () => setPathname(window.location.pathname);
    window.addEventListener("popstate", update);
    return () => window.removeEventListener("popstate", update);
  }, []);
  return pathname;
}
