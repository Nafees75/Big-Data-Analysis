from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSortAthletesById(MRJob):

    def configure_args(self):
        super(MRSortAthletesById, self).configure_args()
        self.add_passthru_arg('--outfile', help='Specify the output file path')

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        # Ensure the 'line' is decoded properly
        line = line.strip()
        components = line.split(';')
        athlete_id = components[0].strip()  # Get athlete ID
        # Yield athlete ID with zero-filling for numeric sorting, and the rest of the data
        yield athlete_id.zfill(10), ';'.join(components[1:]).strip()

    def reducer(self, key, values):
        # Ensure correct Unicode handling by stripping leading zeros and correctly encoding output
        for value in values:
            # Emit the athlete ID (without leading zeros) and the associated data
            yield key.lstrip('0'), value

if __name__ == '__main__':
    MRSortAthletesById.run()
