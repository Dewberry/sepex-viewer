export default function JobIdLink({ jobID, onClick }) {
  return (
    <button
      type="button"
      onClick={() => onClick(jobID)}
      title={jobID}
      className="cursor-pointer text-left font-mono text-xs font-semibold hover:underline"
    >
      {jobID.slice(-8)}
    </button>
  );
}
