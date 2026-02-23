import ResultsTabs from "../../components/ResultsTabs";

export default function ResultsLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-6">
      <ResultsTabs />
      {children}
    </div>
  );
}
