from mrjob.job import MRJob
from mrjob.step import MRStep

class TopEventsByDecade(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_sum_medals),
            MRStep(reducer=self.reducer_collect_by_decade),
            MRStep(reducer=self.reducer_sort_decades_and_events)
        ]

    def mapper(self, _, line):
        # Parse the line by the format: athlete_id; country; year; event; medal
        parts = line.strip().split(';')
        if len(parts) >= 5:
            year = int(parts[2].strip())
            country = parts[1].strip()
            event = parts[3].strip()
            medal = parts[4].strip()

            # Determine the decade for the event
            decade = (year // 10) * 10

            # Emit (decade, country, event) and a count of 1 for each medal
            yield (decade, country, event), 1

    def reducer_sum_medals(self, event_key, counts):
        # Sum the medal counts for each event-country pair in a given decade
        total_medals = sum(counts)
        decade, country, event = event_key

        # Emit the decade as the key and the event info and medal count as the value
        yield decade, (country, event, total_medals)

    def reducer_collect_by_decade(self, decade, event_medal_counts):
        # Collect all events for each decade and output them as a list
        event_list = list(event_medal_counts)

        # Emit the decade as key and the list of events with their medal counts
        yield None, (decade, event_list)

    def reducer_sort_decades_and_events(self, _, decade_event_pairs):
        # Sort the decades in descending order
        sorted_decades = sorted(decade_event_pairs, reverse=True, key=lambda x: x[0])

        for decade, event_list in sorted_decades:
            # Sort events by medal count in descending order within each decade
            sorted_events = sorted(event_list, reverse=True, key=lambda x: x[2])[:3]
            for country, event, medal_count in sorted_events:
                yield f"{decade}-{decade+9}", (country, event, medal_count)

if __name__ == '__main__':
    TopEventsByDecade.run()
